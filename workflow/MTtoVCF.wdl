version 1.0


workflow MTtoVCF {
    input {
        String UriMatrixTable
        String OutputBucketFilePath
    }

    call WriteVCF {
        input: 
            PathMT = UriMatrixTable,
            OutputBucketFilePath = OutputBucketFilePath
            
    }
}

    task WriteVCF {
        input {
            String PathMT 
            String OutputBucketFilePath  
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

        curl -O https://raw.githubusercontent.com/evin-padhi/PreprocessVCF/NotebookToWDL/write_vcf.py
        
        python3 ExportVCF.py \
            --MatrixTable ~{PathMT} \
            --OutputBucketFilePath ~{OutputBucketFilePath} 
        
        echo ~{OutputBucketFilePath} > outpath.txt
    >>>

    runtime {
        docker: "quay.io/jonnguye/hail:latest"
        memory: "256G"
        cpu: 64
        disks: "local-disk 1000 SSD"
    }
    
    # uses read_string function to save the output path of 
    # the new VCF to workflow output
    output {
        String PathVCF = read_string('outpath.txt') 
    }

}



