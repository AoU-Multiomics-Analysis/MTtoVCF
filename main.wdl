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
        Int AlleleCountThreshold
        Int AlleleNumberPercentage
        String OutputBucketCheckpointMT
        
        #Filter MTtoVCF parameters
        String OutputBucketVCF

        #Shared params
        String OutputPrefix
        String CloudTmpdir
    }

    String FullPrefix = "~{OutputPrefix}.AC~{AlleleCountThreshold}.AN~{AlleleNumberPercentage}.biallelic"

    call FilterMT.FilterMT as filter {
        input:
            UriMatrixTable = UriMatrixTable,
            SampleList = SampleList,
            AlleleCountThreshold = AlleleCountThreshold,
            AlleleNumberPercentage = AlleleNumberPercentage,
            OutputBucket = OutputBucketCheckpointMT,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir
    }

    call MTtoVCF.MTtoVCF as export {
        input:
            UriMatrixTable = filter.FilteredMT,
            OutputBucket = OutputBucketVCF,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir
    }
        
    output {
        String PathVCF = export.PathVCF 
    }
}







