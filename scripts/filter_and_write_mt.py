import hail as hl
import argparse


# init
def main(args):
    # Initialize Hail with hard-coded configuration
    hl.init(
    app_name='hail_job',
    master='local[*]',
    tmp_dir=f'{args.CloudTmpdir}',  # Cloud storage recommended here
    spark_conf={
        'spark.local.dir': '/cromwell_root',  # Local SSD for Spark shuffle/spill
        'spark.executor.instances': '4',
        'spark.executor.cores': '16',
        'spark.executor.memory': '64g',
        'spark.driver.memory': '64g',
        'spark.sql.shuffle.partitions': '100',
        'spark.default.parallelism': '100',
        'spark.memory.fraction': '0.8',
        'spark.memory.storageFraction': '0.2'
    }
    )
    hl.default_reference('GRCh38')

    # Load matrix table and samples table
    mt = hl.read_matrix_table(args.MatrixTable)
    samples_ht = hl.import_table(args.SampleList, key='research_id')

    # Load pre-computed VAT Hail table for annotation.
    # This table is created from the AoU Variant Annotation Table (VAT) using
    # the TSVtoHailTable workflow.
    vat_ht = hl.read_table(args.VATHailTable)

    # Parse the variant identifier (vid) into locus + alleles so we can join
    # with the matrix table.  Expected vid format: contig-position-ref-alt
    # Use maxsplit=3 so dashes inside allele strings are preserved.
    # The VAT may use contigs without the 'chr' prefix (e.g. '1' instead of
    # 'chr1'), so add the prefix when it is missing to match GRCh38.
    vat_ht = vat_ht.annotate(_parts=vat_ht.vid.split('-', 4))
    vat_ht = vat_ht.annotate(
        locus=hl.locus(
            hl.if_else(
                vat_ht._parts[0].startswith('chr'),
                vat_ht._parts[0],
                'chr' + vat_ht._parts[0]
            ),
            hl.int32(vat_ht._parts[1]),
            reference_genome='GRCh38'),
        alleles=[vat_ht._parts[2], vat_ht._parts[3]]
    )

    # Fixed set of VAT fields to carry forward as annotations.
    # All VAT columns arrive as strings (imported from TSV); the helpers below
    # cast each value to the appropriate type and convert empty strings to
    # Hail missing so that VCF output renders them as '.' rather than ''.
    def _cast_to_int(expr):
        return hl.or_missing(expr != '', hl.int32(expr))

    def _cast_to_float(expr):
        return hl.or_missing(expr != '', hl.float64(expr))

    def _cast_to_str(expr):
        return hl.or_missing(expr != '', expr)
    def sanitize_info(expr):
        expr = hl.or_missing(expr != '', expr)
        expr = hl.if_else(hl.is_defined(expr), expr.replace(';', '|'), expr)
        expr = hl.if_else(hl.is_defined(expr), expr.replace('=', ':'), expr)
        return expr


    vat_ht = vat_ht.select(
        'locus',
        'alleles',
        # GVS population frequencies
        gvs_all_ac=_cast_to_int(vat_ht.gvs_all_ac),
        gvs_all_an=_cast_to_int(vat_ht.gvs_all_an),
        gvs_all_af=_cast_to_float(vat_ht.gvs_all_af),
        gvs_max_ac=_cast_to_int(vat_ht.gvs_max_ac),
        gvs_max_an=_cast_to_int(vat_ht.gvs_max_an),
        gvs_max_af=_cast_to_float(vat_ht.gvs_max_af),
        gvs_max_subpop=_cast_to_str(vat_ht.gvs_max_subpop),
    
        # GVS subpopulation frequencies
        gvs_afr_af=_cast_to_float(vat_ht.gvs_afr_af),
        gvs_amr_af=_cast_to_float(vat_ht.gvs_amr_af),
        gvs_eas_af=_cast_to_float(vat_ht.gvs_eas_af),
        gvs_eur_af=_cast_to_float(vat_ht.gvs_eur_af),
        gvs_mid_af=_cast_to_float(vat_ht.gvs_mid_af),
        gvs_sas_af=_cast_to_float(vat_ht.gvs_sas_af),
        gvs_oth_af=_cast_to_float(vat_ht.gvs_oth_af),

        # gnomAD population frequencies
        gnomad_all_ac=_cast_to_int(vat_ht.gnomad_all_ac),
        gnomad_all_an=_cast_to_int(vat_ht.gnomad_all_an),
        gnomad_all_af=_cast_to_float(vat_ht.gnomad_all_af),
        gnomad_max_ac=_cast_to_int(vat_ht.gnomad_max_ac),
        clinvar_phenotype=_cast_to_str(vat_ht.clinvar_phenotype),
        gnomad_max_an=_cast_to_int(vat_ht.gnomad_max_an),
        gnomad_max_af=_cast_to_float(vat_ht.gnomad_max_af),
        gnomad_max_subpop=_cast_to_str(vat_ht.gnomad_max_subpop),
        # Clinical / functional annotations
        clinvar_classification=_cast_to_str(vat_ht.clinvar_classification),
        clinvar_phenotype=sanitize_info(vat_ht.clinvar_phenotype), 
        omim_phenotypes_id=_cast_to_str(vat_ht.omim_phenotypes_id),
        gene_omim_id=_cast_to_str(vat_ht.gene_omim_id),
        consequence=_cast_to_str(vat_ht.consequence),
        revel=_cast_to_float(vat_ht.revel),
        aa_change=_cast_to_str(vat_ht.aa_change),
        rsid=_cast_to_str(vat_ht.dbsnp_rsid),
        # SpliceAI scores and distances
        splice_ai_acceptor_gain_score=_cast_to_float(vat_ht.splice_ai_acceptor_gain_score),
        splice_ai_acceptor_loss_score=_cast_to_float(vat_ht.splice_ai_acceptor_loss_score),
        splice_ai_donor_gain_score=_cast_to_float(vat_ht.splice_ai_donor_gain_score),
        splice_ai_donor_loss_score=_cast_to_float(vat_ht.splice_ai_donor_loss_score),
        splice_ai_acceptor_gain_distance=_cast_to_int(vat_ht.splice_ai_acceptor_gain_distance),
        splice_ai_acceptor_loss_distance=_cast_to_int(vat_ht.splice_ai_acceptor_loss_distance),
        splice_ai_donor_gain_distance=_cast_to_int(vat_ht.splice_ai_donor_gain_distance),
        splice_ai_donor_loss_distance=_cast_to_int(vat_ht.splice_ai_donor_loss_distance)
    )
    vat_ht = vat_ht.key_by('locus', 'alleles')

    if args.BedFile:
        bed = hl.import_table(args.BedFile, delimiter='\t', no_header=True,
                              types={'f1': hl.tint32, 'f2': hl.tint32})
        bed = bed.rename({'f0': 'contig', 'f1': 'start', 'f2': 'end'})
    
        rg = hl.get_reference('GRCh38')
        lengths = hl.literal(rg.lengths)
    
        bed = bed.annotate(
            start=hl.max(0, bed.start),
            end=hl.min(bed.end, lengths.get(bed.contig))
        )

        bed = bed.filter(
            hl.is_defined(lengths.get(bed.contig)) & (bed.start < bed.end)
        )
            
        regions = bed.annotate(interval=hl.interval(
            hl.locus(bed.contig, bed.start + 1, reference_genome='GRCh38'),
            hl.locus(bed.contig, bed.end, reference_genome='GRCh38'),
            includes_start=True,
            includes_end=False
        )).key_by('interval')
    
        mt = mt.filter_rows(hl.is_defined(regions[mt.locus]))
        
    # Filter matrix table to samples in samples_ht
    mt_filtered = mt.filter_cols(hl.is_defined(samples_ht[mt.s]))
    # Filter for biallelic
    mt_filtered = mt_filtered.filter_rows(hl.len(mt_filtered.alleles) == 2)
    # remove empty GT
    mt_filtered = mt_filtered.filter_rows(hl.agg.any(hl.is_defined(mt_filtered.GT)))
    # remove missing AC
    mt_filtered = mt_filtered.filter_rows(~hl.is_missing(mt_filtered.info.AC))
    # filter out bad quals
    mt_filtered = mt_filtered.filter_rows(hl.is_missing(mt_filtered.filters))

    # add variant qc stats
    mt_filtered = hl.variant_qc(mt_filtered)
    # save off total qc
    mt_filtered = mt_filtered.annotate_rows(
        total = mt_filtered.info.annotate(
            ALL_p_value_hwe = mt_filtered.variant_qc.p_value_hwe,
            ALL_p_value_excess_het = mt_filtered.variant_qc.p_value_excess_het
        )
    )
    # recalculate AC/AF/AN on new filtered set
    mt_filtered = mt_filtered.annotate_rows( info = hl.agg.call_stats(mt_filtered.GT, mt_filtered.alleles) )

    # Allele number percentage cutoff
    mt_filtered = mt_filtered.filter_rows(mt_filtered.info.AN >= int(args.AlleleNumberPercentage)/100 * mt_filtered.count_cols() * 2)
    
    # filter by allele count
    mt_filtered = mt_filtered.filter_rows(
        (hl.min(mt_filtered.info.AC) >= int(args.MinAlleleCount)) &
        (hl.min(mt_filtered.info.AC) <= int(args.MaxAlleleCount))
    )

    # Join filtered MT with VAT table for annotations
    mt_filtered = mt_filtered.annotate_rows(_vat = vat_ht[mt_filtered.row_key])

    # save to info field to export to vcf
    mt_filtered = mt_filtered.annotate_rows(
            info = mt_filtered.info.annotate(
                ALL_p_value_hwe = mt_filtered.total.ALL_p_value_hwe,
                ALL_p_value_excess_het = mt_filtered.total.ALL_p_value_excess_het,

                # add variant stats based on multi-omics cohort
                AF = hl.min(mt_filtered.info.AF),
                AC = hl.min(mt_filtered.info.AC),
                AN = mt_filtered.info.AN,

                # GVS population frequencies from VAT
                gvs_all_ac=mt_filtered._vat.gvs_all_ac,
                gvs_all_an=mt_filtered._vat.gvs_all_an,
                gvs_all_af=mt_filtered._vat.gvs_all_af,
                gvs_max_ac=mt_filtered._vat.gvs_max_ac,
                gvs_max_an=mt_filtered._vat.gvs_max_an,
                gvs_max_af=mt_filtered._vat.gvs_max_af,
                gvs_max_subpop=mt_filtered._vat.gvs_max_subpop,
                
                # GVS subpopulations from VAT
                gvs_afr_af=mt_filtered._vat.gvs_afr_af,
                gvs_amr_af=mt_filtered._vat.gvs_amr_af,
                gvs_eas_af=mt_filtered._vat.gvs_eas_af,
                gvs_eur_af=mt_filtered._vat.gvs_eur_af,
                gvs_mid_af=mt_filtered._vat.gvs_mid_af,
                gvs_sas_af=mt_filtered._vat.gvs_sas_af,
                gvs_oth_af=mt_filtered._vat.gvs_oth_af,

                # gnomAD population frequencies from VAT
                gnomad_all_ac=mt_filtered._vat.gnomad_all_ac,
                gnomad_all_an=mt_filtered._vat.gnomad_all_an,
                gnomad_all_af=mt_filtered._vat.gnomad_all_af,
                gnomad_max_ac=mt_filtered._vat.gnomad_max_ac,
                gnomad_max_an=mt_filtered._vat.gnomad_max_an,
                gnomad_max_af=mt_filtered._vat.gnomad_max_af,
                gnomad_max_subpop=mt_filtered._vat.gnomad_max_subpop,

                # Clinical / functional annotations from VAT
                clinvar_classification=mt_filtered._vat.clinvar_classification,
                clinvar_phenotype=mt_filtered._vat.clinvar_phenotype, 
                omim_phenotypes_id=mt_filtered._vat.omim_phenotypes_id,
                gene_omim_id=mt_filtered._vat.gene_omim_id,
                consequence=mt_filtered._vat.consequence,
                revel=mt_filtered._vat.revel,
                aa_change=mt_filtered._vat.aa_change,
                rsid=mt_filtered._vat.rsid,

                # SpliceAI scores and distances from VAT
                splice_ai_acceptor_gain_score=mt_filtered._vat.splice_ai_acceptor_gain_score,
                splice_ai_acceptor_loss_score=mt_filtered._vat.splice_ai_acceptor_loss_score,
                splice_ai_donor_gain_score=mt_filtered._vat.splice_ai_donor_gain_score,
                splice_ai_donor_loss_score=mt_filtered._vat.splice_ai_donor_loss_score,
                splice_ai_acceptor_gain_distance=mt_filtered._vat.splice_ai_acceptor_gain_distance,
                splice_ai_acceptor_loss_distance=mt_filtered._vat.splice_ai_acceptor_loss_distance,
                splice_ai_donor_gain_distance=mt_filtered._vat.splice_ai_donor_gain_distance,
                splice_ai_donor_loss_distance=mt_filtered._vat.splice_ai_donor_loss_distance,
            )
        ).drop("_vat")
    # get rid of unneeded fields for matrix table save
    mt_filtered = mt_filtered.drop("variant_qc","total")
    

    # Directly export to VCF
    OutputFilePath = f'{args.OutputBucket}/{args.OutputPrefix}.vcf.bgz'
    hl.export_vcf(mt_filtered, OutputFilePath)

    with open('outpath.txt', 'w') as file:
        file.write(OutputFilePath)

    hl.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter and write Hail MatrixTable with hard-coded Hail configuration.")
    parser.add_argument("--MatrixTable", required=True, help="Path to input MatrixTable.")
    parser.add_argument("--SampleList", required=True, help="Path to samples TSV file.")
    parser.add_argument("--MinAlleleCount", required=True, help="Min Allele count threshold.")
    parser.add_argument("--MaxAlleleCount", required=True, help="Max Allele count threshold.")
    parser.add_argument("--BedFile", required=False, help="Bed file containing regions of interest, typically cis windows for genes")
    parser.add_argument("--AlleleNumberPercentage", required=True, help="Allele number percentage cutoff.")
    parser.add_argument("--VATHailTable", required=True, help="Path to pre-computed Hail table from AoU VAT with per-ancestry and cohort AC/AN/AF columns")
    parser.add_argument("--OutputBucket", required=True, help="Path to output VCF bucket.")
    parser.add_argument("--OutputPrefix", required=True, help="Output prefix.")
    parser.add_argument("--CloudTmpdir", required=True, help="Temporary directory for spark/hail to work with.")

    args = parser.parse_args()
    main(args)
