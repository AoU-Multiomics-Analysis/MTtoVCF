version 1.0


workflow ComputeGeneticPCs {
    input {
        String UriMatrixTable
        String OutputBucket 
        String OutputPrefix
    }

    call ComputeGeneticPCsTask {
        input: 
            PathMT = UriMatrixTable,
            OutputBucket = OutputBucket
            OutputPrefix = OutputPrefix
            
    }
}


task ComputeGeneticPCsTask {
    input {


    }
    
    command <<<


    >>>
    
    runtime {



    }

    output {


    }
}


