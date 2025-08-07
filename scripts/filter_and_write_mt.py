import hail as hl
import argparse

def main(args):
    # Initialize Hail with hard-coded configuration
    hl.init(
    app_name='hail_job',
    master='local[*]',
    #gs://fc-secure-b8771cfd-5455-4292-a720-8533eb501a93
    tmp_dir=f'{args.cloud_tempdir}/hail-tmp/',  # Cloud storage recommended here
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
    },
    default_reference='GRCh38'
    )

    # Load matrix table and samples table
    mt = hl.read_matrix_table(args.matrix_table)
    samples_ht = hl.import_table(args.samples_table, key='research_id')

    # Filter matrix table to samples in samples_ht
    mt_filtered = mt.filter_cols(hl.is_defined(samples_ht[mt.s]))
    mt_filtered = mt_filtered.filter_rows(hl.len(mt_filtered.alleles) == 2)
    mt_filtered = mt_filtered.filter_rows(hl.agg.any(hl.is_defined(mt_filtered.GT)))
    mt_filtered = mt_filtered.filter_rows(~hl.is_missing(mt_filtered.info.AC))

    # Write filtered matrix table to output checkpoint
    mt_filtered.write(args.output_checkpoint, overwrite=True)

    hl.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter and write Hail MatrixTable with hard-coded Hail configuration.")
    parser.add_argument("--matrix_table", required=True, help="Path to input MatrixTable.")
    parser.add_argument("--samples_table", required=True, help="Path to samples TSV file.")
    parser.add_argument("--output_checkpoint", required=True, help="Path to output checkpoint MatrixTable.")
    parser.add_argument("--cloud_tmpdir", required=True, help="Temporary directory for spark/hail to work with. Prefix with gs://")

    args = parser.parse_args()
    main(args)
