version 1.0

workflow TSVtoHailTable {
    meta {
        author: "Jonathan Nguyen"
        description: "Stream a tsv.gz from a cloud path and convert it into a Hail table."
    }

    input {
        # Path to the input tsv.gz in cloud storage (e.g. gs://bucket/file.tsv.gz)
        String InputTSVPath

        # Cloud path where the output Hail table will be written (e.g. gs://bucket/output.ht)
        String OutputTablePath

        # Temporary cloud directory for Spark/Hail intermediate data
        String CloudTmpdir

        # Optional: field name to use as the Hail table key
        String? KeyField

        # Set to true when the .gz file is block-gzipped (.bgz) to enable
        # parallel partition reads for better streaming performance on large files
        Boolean ForceBGZ = false

        # Docker image branch tag
        String Branch = "main"
    }

    call ImportTSV {
        input:
            InputTSVPath  = InputTSVPath,
            OutputTablePath = OutputTablePath,
            CloudTmpdir   = CloudTmpdir,
            KeyField      = KeyField,
            ForceBGZ      = ForceBGZ,
            Branch        = Branch
    }

    output {
        String HailTablePath = ImportTSV.HailTablePath
    }
}

task ImportTSV {
    input {
        String  InputTSVPath
        String  OutputTablePath
        String  CloudTmpdir
        String? KeyField
        Boolean ForceBGZ
        String  Branch
    }

    command <<<
        set -e

        export SPARK_LOCAL_DIRS=/cromwell_root

        python3 /tsv_gz_to_hail_table.py \
            --InputPath ~{InputTSVPath} \
            --OutputPath ~{OutputTablePath} \
            --CloudTmpdir ~{CloudTmpdir} \
            ~{if defined(KeyField) then "--KeyField " + select_first([KeyField]) else ""} \
            ~{if ForceBGZ then "--ForceBGZ" else ""}
    >>>

    runtime {
        # Docker image for this repo – contains Hail and all required dependencies.
        # The same image is reused across all workflows in this repository.
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf:" + Branch
        memory: "256G"
        cpu: 64
        disks: "local-disk 1000 SSD"
    }

    meta {
        author: "Jonathan Nguyen"
    }

    output {
        String HailTablePath = read_string('outpath.txt')
    }
}
