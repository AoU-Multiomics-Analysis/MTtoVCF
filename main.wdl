version 1.0

import "workflow/FilterMT.wdl" as FilterMT
import "workflow/VCFPostProcess.wdl" as VCFPostProcess

task IndexVCF {
    input {
        File VCF
        String Prefix
    }
   
    command <<<
        set -euo pipefail
        bcftools index --tbi --force "~{VCF}"
    >>>
    
    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils" 
        memory: "256G"
        cpu: 64
        disks: "local-disk 1000 SSD"
    }
    
    output {
        File Index =  "~{Prefix}.vcf.bgz.tbi"
    }
}


workflow FilterMTAndExportToVCF{
    meta {
            author: "Jonathan Nguyen"
    }
    
    input {
        #FilterMT parameters
        String UriMatrixTable
        File SampleList
        File? BedFile
        String? VATHailTable
        Boolean AnnotateWithVAT = true
        Int MinAlleleCountThreshold = 5
        Int MaxAlleleCountThreshold = 10000000
        Int AlleleNumberPercentage = 95
        String SampleSetName
        String CallSetName
        
        #Shared params
        String OutputBucket
        String OutputPrefix
        String CloudTmpdir
        String Branch = "main"

        # Runtime params for the Hail filter task
        Int FilterTaskCpu = 64
        String FilterTaskMemory = "256G"
        String FilterTaskDisk = "local-disk 1000 SSD"
        String FilterSparkDriverMemory = "64g"
        Int FilterSparkParallelism = 100
        Int FilterSparkShufflePartitions = 100

        # Optional VCF post-processing
        Boolean MakeDosage = false
        Boolean MakePlink = false
        Int DosageThreads = 4
        Int PlinkNewIdMaxAlleleLen = 200
    }

    String FullPrefix = "~{OutputPrefix}.~{SampleSetName}.AC~{MinAlleleCountThreshold}.AN~{AlleleNumberPercentage}.biallelic.~{CallSetName}"

    call FilterMT.FilterMT as filter {
        input:
            UriMatrixTable = UriMatrixTable,
            SampleList = SampleList,
            MinAlleleCountThreshold = MinAlleleCountThreshold,
            MaxAlleleCountThreshold = MaxAlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            VATHailTable = VATHailTable,
            AnnotateWithVAT = AnnotateWithVAT,
            OutputBucket = OutputBucket,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir,
            BedFile = BedFile,
            Branch = Branch,
            TaskCpu = FilterTaskCpu,
            TaskMemory = FilterTaskMemory,
            TaskDisk = FilterTaskDisk,
            SparkDriverMemory = FilterSparkDriverMemory,
            SparkParallelism = FilterSparkParallelism,
            SparkShufflePartitions = FilterSparkShufflePartitions
    }

   call IndexVCF {
        input:
            VCF = filter.PathVCF,
            Prefix = FullPrefix
    }

   call VCFPostProcess.VCFPostProcess as postprocess {
        input:
            vcf_file = filter.PathVCF,
            output_prefix = FullPrefix,
            make_dosage = MakeDosage,
            make_plink = MakePlink,
            dosage_threads = DosageThreads,
            plink_new_id_max_allele_len = PlinkNewIdMaxAlleleLen
    }

    output {
        File PathVCF = filter.PathVCF
        File Index = IndexVCF.Index
        File? GenotypeDosage = postprocess.GenotypeDosage
        File? GenotypeDosageIndex = postprocess.GenotypeDosageIndex
        File? PlinkPgen = postprocess.PlinkPgen
        File? PlinkPvar = postprocess.PlinkPvar
        File? PlinkPsam = postprocess.PlinkPsam
    }
}










