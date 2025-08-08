version 1.0


workflow FilterMT {
    input {
        String UriMatrixTable
        File SampleList
        Int AlleleCountThreshold
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
    }
    
    call TaskFilterMT {
        input:
            UriMatrixTable = UriMatrixTable, 
            SampleList = SampleList,
            AlleleCountThreshold = AlleleCountThreshold,
            OutputBucket = OutputBucket,
            OutputPrefix = OutputPrefix,
            CloudTmpdir = CloudTmpdir
    }

    output {
        String FilteredMT = TaskFilterMT.FilteredMT
    }
}
task TaskFilterMT {
    input {
        String UriMatrixTable
        File SampleList
        Int AlleleCountThreshold
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
    }
    command <<<
        export SPARK_LOCAL_DIRS=/cromwell_root

        echo "Checking disk mounts and usage:"
        df -h
        echo "Checking Spark local directory:"
        echo $SPARK_LOCAL_DIRS
        echo "Checking /cromwell_root directory:"
        ls -lah /cromwell_root

        curl -O https://raw.githubusercontent.com/AoU-Multiomics-Analysis/MTtoVCF/refs/heads/develop/scripts/filter_and_write_mt.py

        # writes VCF to bucket path 
        # and also generates outpath.txt upon completion 
        # of writing VCF 
        python3 filter_and_write_mt.py \
            --MatrixTable ~{UriMatrixTable} \
            --SampleList ~{SampleList} \
            --AlleleCount ~{AlleleCountThreshold} \
            --OutputBucket ~{OutputBucket} \
            --OutputPrefix ~{OutputPrefix} \
            --CloudTmpdir ~{CloudTmpdir}


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
        String FilteredMT = read_string('outpath.txt') 
    }
}
