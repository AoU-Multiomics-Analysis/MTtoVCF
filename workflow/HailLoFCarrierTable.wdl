version 1.0

workflow HailLoFCarrierTable {
    input {
        String UriMatrixTable
        File SampleList
        String VATHailTable
        File? BedFile
        Int MinAlleleCountThreshold = 5
        Int MaxAlleleCountThreshold = 1000000000
        Int AlleleNumberPercentage = 95
        String OutputBucket
        String OutputPrefix
        String CloudTmpdir
        String Branch = "main"

        Int TaskCpu = 64
        String TaskMemory = "256G"
        String TaskDisk = "local-disk 1000 SSD"
        String SparkDriverMemory = "64g"
        Int SparkParallelism = 100
        Int SparkShufflePartitions = 100
    }

    call ExtractHailLoFCarriers {
        input:
            UriMatrixTable = UriMatrixTable,
            SampleList = SampleList,
            VATHailTable = VATHailTable,
            BedFile = BedFile,
            MinAlleleCountThreshold = MinAlleleCountThreshold,
            MaxAlleleCountThreshold = MaxAlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            OutputBucket = OutputBucket,
            OutputPrefix = OutputPrefix,
            CloudTmpdir = CloudTmpdir,
            Branch = Branch,
            TaskCpu = TaskCpu,
            TaskMemory = TaskMemory,
            TaskDisk = TaskDisk,
            SparkDriverMemory = SparkDriverMemory,
            SparkParallelism = SparkParallelism,
            SparkShufflePartitions = SparkShufflePartitions
    }

    output {
        File LoFCarriersHC = ExtractHailLoFCarriers.lof_carriers_hc
        File LoFCarriersHCOrLC = ExtractHailLoFCarriers.lof_carriers_hc_or_lc
    }
}

task ExtractHailLoFCarriers {
    input {
        String UriMatrixTable
        File SampleList
        File? BedFile
        String VATHailTable
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

        python3 /extract_lof_carriers_hail.py ~{if defined(BedFile) then "--BedFile " + BedFile else ""} \
            --MatrixTable ~{UriMatrixTable} \
            --SampleList ~{SampleList} \
            --VATHailTable ~{VATHailTable} \
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
        File lof_carriers_hc = read_string("lof_carriers_hc_outpath.txt")
        File lof_carriers_hc_or_lc = read_string("lof_carriers_hc_or_lc_outpath.txt")
    }
}
