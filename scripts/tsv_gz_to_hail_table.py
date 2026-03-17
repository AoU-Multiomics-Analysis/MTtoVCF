import hail as hl
import argparse
import os
import subprocess


def main(args):
    hl.init(
        app_name='hail_job',
        master='local[*]',
        tmp_dir=args.CloudTmpdir,
        spark_conf={
            'spark.local.dir': '/cromwell_root',
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

    print(f'Streaming TSV from: {args.InputPath}', flush=True)
    print("Disk usage:", flush=True)
    subprocess.run(['df', '-h'], check=False)

    # Import the TSV directly from cloud storage.
    # Hail streams the file without downloading it locally first.
    # Setting force_bgz=True allows parallel block-level reads when the file
    # is block-gzipped (.bgz) but named with a .gz extension, which greatly
    # improves throughput for very large files.
    ht = hl.import_table(
        args.InputPath,
        force_bgz=args.ForceBGZ
    )

    if args.KeyField:
        ht = ht.key_by(args.KeyField)

    print(f'Writing Hail table to: {args.OutputPath}', flush=True)
    ht.write(args.OutputPath, overwrite=True)

    hl.stop()

    with open('outpath.txt', 'w') as f:
        f.write(args.OutputPath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream a tsv.gz from a cloud path and convert it to a Hail table."
    )
    parser.add_argument(
        "--InputPath",
        required=True,
        help="Cloud path to the input tsv.gz file (e.g. gs://bucket/file.tsv.gz)."
    )
    parser.add_argument(
        "--OutputPath",
        required=True,
        help="Cloud path where the output Hail table will be written (e.g. gs://bucket/output.ht)."
    )
    parser.add_argument(
        "--CloudTmpdir",
        required=True,
        help="Temporary cloud directory for Spark/Hail intermediate data."
    )
    parser.add_argument(
        "--KeyField",
        required=False,
        default=None,
        help="Field name to use as the Hail table key. If omitted the table is unkeyed."
    )
    parser.add_argument(
        "--ForceBGZ",
        action='store_true',
        help=(
            "Treat the .gz file as block-gzip (.bgz) to enable parallel partition reads. "
            "Use when the input file is block-gzipped but carries a .gz extension."
        )
    )

    args = parser.parse_args()
    main(args)
