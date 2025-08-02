import hail as hl
import argparse
import os

def write_vcf(inputs):

    OutPath = inputs['']
    #LOAD TABLES AND FIND SUBSET
    print('hail reading matrix table')
    mt = hl.read_matrix_table(inputs['MatrixTable'])
    
    print('Loading sample table')
    samples_table = hl.import_table(inputs['SampleList'], key='research_id')
   
    print('Filtering matrix table by sample table')
    mt = mt.filter_cols(hl.is_defined(samples_table[mt.s]))
    print(f"Filtering to {mt.count_cols()} samples")

    #REMOVE MULTIALLELIC
    print('removing multi allelic sites')
    mt = mt.filter_rows(~mt.was_split)

    #HAS GT
    mt = mt.filter_rows(hl.agg.any(hl.is_defined(mt.GT)))

    #Checkpoint for initial filtering
    print("First Checkpoint:", flush=True)
    mt = mt.checkpoint(f"{inputs['cloud_checkpoint_dir']}/filtered.mt", overwrite=True)
    
    #ONLY CONTAINS PASS IN FT
    #IF FT is not pass, set to 0,0
    mt = mt.annotate_entries(GT = hl.if_else(hl.is_defined(mt.FT) & (mt.FT == "PASS"),mt.GT,hl.call(0, 0)))

    #Save total pop data
    mt = mt.annotate_rows(
        total = mt.info.annotate(
            ALL_AF = hl.min(mt.info.AF),
            ALL_AN = mt.info.AN,
            ALL_AC = hl.min(mt.info.AC),
            ALL_p_value_hwe = mt.variant_qc.p_value_hwe,
            ALL_p_value_excess_het = mt.variant_qc.p_value_excess_het
        )
    )
    
    #OVERWRITE TOTAL POPULATION INFO WITH SUBPOPULATION INFO
    mt = mt.annotate_rows( info = hl.agg.call_stats(mt.GT, mt.alleles) )

    #Checkpoint for qc stats
    print("Second Checkpoint:", flush=True)
    mt = mt.checkpoint(f"{inputs['cloud_checkpoint_dir']}/qc_stats.mt", overwrite=True)
    
    #95% of alleles called in the population
    mt = mt.filter_rows(mt.info.AN >= 0.95 * mt.count_cols() * 2)

    #recalculate per subpopulation
    mt = hl.variant_qc(mt)

    #only show minor allele data
    mt = mt.annotate_rows(
        info = mt.info.annotate(
            ALL_AF = mt.total.ALL_AF,
            ALL_AC = mt.total.ALL_AC,
            ALL_AN = mt.total.ALL_AN,
            ALL_p_value_hwe = mt.total.ALL_p_value_hwe,
            ALL_p_value_excess_het = mt.total.ALL_p_value_excess_het,
            AF = hl.min(mt.info.AF),
            AC = hl.min(mt.info.AC),
            AN = mt.info.AN,
            p_value_hwe = mt.variant_qc.p_value_hwe,
            p_value_excess_het = mt.variant_qc.p_value_excess_het
        )
    )

    #FILTER BY MIN AC
    mt = mt.filter_rows(mt.info.AC >= inputs['AlleleCountThreshold'])

    #Checkpoint for second filter
    print("Third Checkpoint:", flush=True)
    mt = mt.checkpoint(f"{inputs['cloud_checkpoint_dir']}/second_filter.mt", overwrite=True)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--MatrixTable", required=True)
    parser.add_argument("--SampleList", required=True)
    parser.add_argument("--AlleleCountThreshold", type=int, required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--cloud_checkpoint_dir", required=True)

    args = parser.parse_args()

    inputs = {
        'MatrixTable': args.matrix_table,
        'SampleList': args.samples_table,
        'AlleleCountThreshold': args.MinimumAC_inclusive,
        'output_path': args.output_path,
        'cloud_checkpoint_dir': args.cloud_checkpoint_dir
    }

    hl.init(
        app_name='hail_job',
        master='local[*]',
        spark_conf={
            'spark.executor.instances': '4',
            'spark.executor.cores': '8',
            'spark.executor.memory': '25g',
            'spark.driver.memory': '30g',
            'spark.local.dir': '/cromwell_root',
            'spark.sql.shuffle.partitions': '100',
            'spark.default.parallelism': '100',
            'spark.memory.fraction': '0.8',
            'spark.memory.storageFraction': '0.2',
        },
        default_reference='GRCh38'
    )
    
    print("Spark local directories:", os.getenv("SPARK_LOCAL_DIRS"), flush=True)
    print("Disk usage:", flush=True)
    os.system("df -h")
    
    write_vcf(inputs)
