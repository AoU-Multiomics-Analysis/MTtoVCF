import "workflow/FilterMT.wdl" as FilterMT
import "workflow/MTtoVCF.wdl" as MTtoVCF

workflow FilterMTAndExportToVCF{
    meta {
            author: 'Jonathan Nguyen'
    }
    
    input {
        #FilterMT parameters
        String UriMatrixTable
        File SampleList
        Int AlleleCountThreshold
        String OutputBucketCheckpointMT
        
        #Filter MTtoVCF parameters
        String OutputBucketVCF

        #Shared params
        String OutputPrefix
        String CloudTmpdir
    }


    call FilterMT.FilterMT as filter {
        input:
            UriMatrixTable = UriMatrixTable
            SampleList = SampleList
            AlleleCountThreshold = AlleleCountThreshold
            OutputBucket = OutputBucketCheckpointMT
            OutputPrefix = OutputPrefix
            CloudTmpdir = CloudTmpdir
    }

    call MTtoVCF as export {
        input:
            UriMatrixTable = filter.FilteredMT
            OutputBucket = OutputBucketVCF
            OutputPrefix = OutputPrefix
    }
        
    output {
        String PathVCF = export.PathVCF 
    }
}

