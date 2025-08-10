version 1.0


workflow MTtoVCF {
    input {
        String UriMatrixTable
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
    }

    call WriteVCF {
        input: 
            PathMT = UriMatrixTable,
            OutputBucket = OutputBucket,
            OutputPrefix = OutputPrefix,
            CloudTmpdir = CloudTmpdir
    }

    output {
        String PathVCF = WriteVCF.PathVCF
    }
}

    task WriteVCF {
        input {
            String PathMT 
            String OutputBucket 
            String OutputPrefix
            String CloudTmpdir
        }  
    command <<<
        set -e

        export SPARK_LOCAL_DIRS=/cromwell_root

        echo "Checking disk mounts and usage:"
        df -h
        echo "Checking Spark local directory:"
        echo $SPARK_LOCAL_DIRS
        echo "Checking /cromwell_root directory:"
        ls -lah /cromwell_root

        curl -O https://raw.githubusercontent.com/AoU-Multiomics-Analysis/MTtoVCF/refs/heads/develop/scripts/ExportVCF.py

        # writes VCF to bucket path 
        # and also generates outpath.txt upon completion 
        # of writing VCF 
        python3 ExportVCF.py \
            --MatrixTable ~{PathMT} \
            --OutputBucket ~{OutputBucket} \
            --OutputPrefix ~{OutputPrefix} \
            --CloudTmpdir ~{CloudTmpdir}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf:main"
        memory: "256G"
        cpu: 64
        disks: "local-disk 2000 SSD"
    }
    
    meta {
        author: "Jonathan Nguyen"
    }
    
    output {
        String PathVCF = read_string('outpath.txt') 
    }

}



