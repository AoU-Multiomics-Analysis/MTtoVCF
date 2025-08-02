version 1.0


workflow ComputeGeneticPCs {
    input {
        String UriMatrixTable
        String OutputBucket 
        String OutputPrefix
    }

    call WriteVCF {
        input: 
            PathMT = UriMatrixTable,
            OutputBucket = OutputBucket
            OutputPrefix = OutputPrefix
            
    }
}


