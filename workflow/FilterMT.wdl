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
        String? VATHailTable
        Boolean AnnotateWithVAT = true
        Int TaskCpu = 64
        String TaskMemory = "256G"
        String TaskDisk = "local-disk 1000 SSD"
        String SparkDriverMemory = "64g"
        Int SparkParallelism = 100
        Int SparkShufflePartitions = 100
    }
    
    call TaskFilterMT {
        input:
            UriMatrixTable = UriMatrixTable, 
            SampleList = SampleList,
            MinAlleleCountThreshold = MinAlleleCountThreshold,
            MaxAlleleCountThreshold = MaxAlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            VATHailTable = VATHailTable,
            AnnotateWithVAT = AnnotateWithVAT,
            OutputBucket = OutputBucket,
            OutputPrefix = OutputPrefix,
            CloudTmpdir = CloudTmpdir,
            Branch = Branch,
            BedFile = BedFile,
            TaskCpu = TaskCpu,
            TaskMemory = TaskMemory,
            TaskDisk = TaskDisk,
            SparkDriverMemory = SparkDriverMemory,
            SparkParallelism = SparkParallelism,
            SparkShufflePartitions = SparkShufflePartitions
    }

    output {
        File PathVCF = TaskFilterMT.PathVCF
    }
}

task TaskFilterMT {
    input {
        String UriMatrixTable
        File SampleList
        File? BedFile
        String? VATHailTable
        Boolean AnnotateWithVAT
        Int MinAlleleCountThreshold
        Int MaxAlleleCountThreshold
        Int AlleleNumberPercentage
        String OutputBucket 
        String OutputPrefix
        String CloudTmpdir
        String Branch
        Int TaskCpu
        String TaskMemory
        String TaskDisk
        String SparkDriverMemory
        Int SparkParallelism
        Int SparkShufflePartitions
    }

    command <<<
        export SPARK_LOCAL_DIRS=/cromwell_root

        # writes VCF to bucket path 
        # and also generates outpath.txt upon completion 
        # of writing VCF 
        python3 /filter_and_write_mt.py   ~{if defined(BedFile) then "--BedFile " + BedFile else ""}  \
            ~{if AnnotateWithVAT then "--VATHailTable " + select_first([VATHailTable]) else "--SkipVATAnnotations"} \
            --MatrixTable ~{UriMatrixTable} \
            --SampleList ~{SampleList} \
            --MinAlleleCount ~{MinAlleleCountThreshold} \
            --MaxAlleleCount ~{MaxAlleleCountThreshold} \
            --AlleleNumberPercentage ~{AlleleNumberPercentage} \
            --OutputBucket ~{OutputBucket} \
            --OutputPrefix ~{OutputPrefix} \
            --CloudTmpdir ~{CloudTmpdir} \
            --SparkLocalThreads ~{TaskCpu} \
            --SparkDriverMemory ~{SparkDriverMemory} \
            --SparkParallelism ~{SparkParallelism} \
            --SparkShufflePartitions ~{SparkShufflePartitions}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf:" + Branch
        memory: TaskMemory
        cpu: TaskCpu
        disks: TaskDisk
    }
    
    output {
        File PathVCF = read_string('outpath.txt')
    }
}
