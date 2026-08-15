version 1.0

workflow HailLoFCarrierTable {
    input {
        String UriMatrixTable
        Boolean MatrixTableAlreadyFiltered = false
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

        Int IndexCpu = 4
        String IndexMemory = "32G"
        String IndexDisk = "local-disk 100 SSD"
    }

    call ExtractHailLoFCarriers {
        input:
            UriMatrixTable = UriMatrixTable,
            MatrixTableAlreadyFiltered = MatrixTableAlreadyFiltered,
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

    call IndexLoFVCF {
        input:
            VCF = ExtractHailLoFCarriers.lof_variants_vcf,
            Prefix = OutputPrefix + ".lof_variants",
            TaskCpu = IndexCpu,
            TaskMemory = IndexMemory,
            TaskDisk = IndexDisk
    }

    output {
        File LoFVariantsVCF = ExtractHailLoFCarriers.lof_variants_vcf
        File LoFVariantsVCFIndex = IndexLoFVCF.index
        File LoFCarriersHC = ExtractHailLoFCarriers.lof_carriers_hc
        File LoFCarriersHCOrLC = ExtractHailLoFCarriers.lof_carriers_hc_or_lc
    }
}

task ExtractHailLoFCarriers {
    input {
        String UriMatrixTable
        Boolean MatrixTableAlreadyFiltered
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
            ~{if MatrixTableAlreadyFiltered then "--MatrixTableAlreadyFiltered" else ""} \
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
        File lof_variants_vcf = read_string("lof_variants_vcf_outpath.txt")
        File lof_carriers_hc = read_string("lof_carriers_hc_outpath.txt")
        File lof_carriers_hc_or_lc = read_string("lof_carriers_hc_or_lc_outpath.txt")
    }
}

task IndexLoFVCF {
    input {
        File VCF
        String Prefix
        Int TaskCpu
        String TaskMemory
        String TaskDisk
    }

    command <<<
        set -euo pipefail

        bcftools index --tbi --force \
            --output "~{Prefix}.vcf.bgz.tbi" \
            "~{VCF}"
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:main"
        memory: TaskMemory
        cpu: TaskCpu
        disks: TaskDisk
    }

    output {
        File index = "~{Prefix}.vcf.bgz.tbi"
    }
}
