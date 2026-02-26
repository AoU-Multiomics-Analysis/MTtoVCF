version 1.0

import "workflow/FilterMT.wdl" as FilterMT
import "workflow/MTtoVCF.wdl" as MTtoVCF

workflow FilterMTAndExportToVCF{
    meta {
            author: "Jonathan Nguyen"
    }
    
    input {
        #FilterMT parameters
        String UriMatrixTable
        File SampleList
        File? BedFile
        String AncestryAssignments
        Int MinAlleleCountThreshold = 5
        Int MaxAlleleCountThreshold = 10000000
        Int AlleleNumberPercentage = 95
        String OutputBucketCheckpointMT
        String SampleSetName
        String CallSetName
        
        #Filter MTtoVCF parameters
        String OutputBucketVCF

        #Shared params
        String OutputPrefix
        String CloudTmpdir
        String Branch = "main"
    }

    String FullPrefix = "~{OutputPrefix}.~{SampleSetName}.AC~{MinAlleleCountThreshold}.AN~{AlleleNumberPercentage}.biallelic.~{CallSetName}"

    call FilterMT.FilterMT as filter {
        input:
            UriMatrixTable = UriMatrixTable,
            SampleList = SampleList,
            MinAlleleCountThreshold = MinAlleleCountThreshold,
            MaxAlleleCountThreshold = MaxAlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            AncestryAssignments = AncestryAssignments,
            OutputBucket = OutputBucketCheckpointMT,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir,
            BedFile = BedFile,
            Branch = Branch
    }

    call MTtoVCF.MTtoVCF as export {
        input:
            UriMatrixTable = filter.FilteredMT,
            OutputBucket = OutputBucketVCF,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir,
            Branch = Branch
    }
        
    output {
        String PathVCF = export.PathVCF 
    }
}










