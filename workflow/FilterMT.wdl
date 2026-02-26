version 1.0

workflow FilterMT {
    input {
        String UriMatrixTable
        File SampleList
        Int MinAlleleCountThreshold = 5
        Int MaxAlleleCountThreshold = 1000000000
        Int AlleleNumberPercentage = 95
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
        String Branch = "main"
        File? BedFile
        String AncestryAssignments
    }
    
    call TaskFilterMT {
        input:
            UriMatrixTable = UriMatrixTable, 
            SampleList = SampleList,
            MinAlleleCountThreshold = MinAlleleCountThreshold,
            MaxAlleleCountThreshold = MaxAlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            AncestryAssignments = AncestryAssignments,
            OutputBucket = OutputBucket,
            OutputPrefix = OutputPrefix,
            CloudTmpdir = CloudTmpdir,
            Branch = Branch,
            BedFile = BedFile
    }

    output {
        String FilteredMT = TaskFilterMT.FilteredMT
    }
}

task TaskFilterMT {
    input {
        String UriMatrixTable
        File SampleList
        File? BedFile
        String AncestryAssignments
        Int MinAlleleCountThreshold
        Int MaxAlleleCountThreshold
        Int AlleleNumberPercentage
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
        String Branch
    }

    command <<<
        export SPARK_LOCAL_DIRS=/cromwell_root

        # writes VCF to bucket path 
        # and also generates outpath.txt upon completion 
        # of writing VCF 
        python3 /filter_and_write_mt.py   ~{if defined(BedFile) then "--BedFile " + BedFile else ""}  \
            --AncestryAssignments ~{AncestryAssignments} \
            --MatrixTable ~{UriMatrixTable} \
            --SampleList ~{SampleList} \
            --MinAlleleCount ~{MinAlleleCountThreshold} \
            --MaxAlleleCount ~{MaxAlleleCountThreshold} \
            --AlleleNumberPercentage ~{AlleleNumberPercentage} \
            --OutputBucket ~{OutputBucket} \
            --OutputPrefix ~{OutputPrefix} \
            --CloudTmpdir ~{CloudTmpdir}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf:" + Branch
        memory: "256G"
        cpu: 64
        disks: "local-disk 1000 SSD"
    }
    
    output {
        String FilteredMT = read_string('outpath.txt') 
    }
}
