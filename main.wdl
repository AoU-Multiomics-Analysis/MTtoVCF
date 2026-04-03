version 1.0

import "workflow/FilterMT.wdl" as FilterMT

task IndexVCF {
    input {
        File VCF
        String Prefix
    }
   
    command <<<
        set -euo pipefail
        bcftools index -p vcf ~{VCF}
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
        String VATHailTable
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
            OutputBucket = OutputBucket,
            OutputPrefix = FullPrefix,
            CloudTmpdir = CloudTmpdir,
            BedFile = BedFile,
            Branch = Branch
    }
        
    
    
   call IndexVCF {
        input: 
            VCF = filter.PathVCF,
            Prefix = FullPrefix
    } 
    
    output {
        String PathVCF = filter.PathVCF
        File Index = IndexVCF.Index
    }
}










