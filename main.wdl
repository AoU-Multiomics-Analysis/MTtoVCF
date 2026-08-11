version 1.0

import "workflow/FilterMT.wdl" as FilterMT
import "workflow/VCFPostProcess.wdl" as VCFPostProcess

task IndexVCF {
    input {
        File VCF
        String IndexDestination
        String UtilsImageTag
        Float IndexDiskMultiplier
        Int IndexDiskOverheadGiB
        Int IndexMinDiskGiB
    }

    Int CalculatedDiskGiB = ceil(size(VCF, "GiB") * IndexDiskMultiplier) + IndexDiskOverheadGiB
    Int IndexDiskGiB = if CalculatedDiskGiB > IndexMinDiskGiB then CalculatedDiskGiB else IndexMinDiskGiB

    command <<<
        /index_vcf.sh "~{VCF}" "~{IndexDestination}"
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:" + UtilsImageTag
        memory: "256G"
        cpu: 64
        disks: "local-disk ~{IndexDiskGiB} SSD"
    }

    output {
        File Index = read_string("index_outpath.txt")
    }
}

task IndexAnnotations {
    input {
        File Annotations
        String IndexDestination
        String UtilsImageTag
        Float IndexDiskMultiplier
        Int IndexDiskOverheadGiB
        Int IndexMinDiskGiB
    }

    Int CalculatedDiskGiB = ceil(size(Annotations, "GiB") * IndexDiskMultiplier) + IndexDiskOverheadGiB
    Int IndexDiskGiB = if CalculatedDiskGiB > IndexMinDiskGiB then CalculatedDiskGiB else IndexMinDiskGiB

    command <<<
        /index_annotations.sh "~{Annotations}" "~{IndexDestination}"
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:" + UtilsImageTag
        memory: "256G"
        cpu: 64
        disks: "local-disk ~{IndexDiskGiB} SSD"
    }

    output {
        File Index = read_string("index_outpath.txt")
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
        String UtilsImageTag = "main"
        Float IndexDiskMultiplier = 2.0
        Int IndexDiskOverheadGiB = 10
        Int IndexMinDiskGiB = 20

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
    String NormalizedOutputBucket = sub(OutputBucket, "/+$", "")
    String VCFIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".vcf.bgz.tbi"
    String AnnotationIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".annotations.tsv.bgz.tbi"

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
            IndexDestination = VCFIndexDestination,
            UtilsImageTag = UtilsImageTag,
            IndexDiskMultiplier = IndexDiskMultiplier,
            IndexDiskOverheadGiB = IndexDiskOverheadGiB,
            IndexMinDiskGiB = IndexMinDiskGiB
    }

   call IndexAnnotations {
        input:
            Annotations = filter.PathAnnotations,
            IndexDestination = AnnotationIndexDestination,
            UtilsImageTag = UtilsImageTag,
            IndexDiskMultiplier = IndexDiskMultiplier,
            IndexDiskOverheadGiB = IndexDiskOverheadGiB,
            IndexMinDiskGiB = IndexMinDiskGiB
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
        File PathAnnotations = filter.PathAnnotations
        File Index = IndexVCF.Index
        File VCFIndex = IndexVCF.Index
        File AnnotationIndex = IndexAnnotations.Index
        File? GenotypeDosage = postprocess.GenotypeDosage
        File? GenotypeDosageIndex = postprocess.GenotypeDosageIndex
        File? PlinkPgen = postprocess.PlinkPgen
        File? PlinkPvar = postprocess.PlinkPvar
        File? PlinkPsam = postprocess.PlinkPsam
    }
}










