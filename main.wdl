import "workflow/FilterMT.wdl" as FilterMT
import "workflow/MTtoVCF.wdl" as MTtoVCF

workflow wf_featurecount{
    meta {
            author: 'Jonathan Nguyen'
    }

    call FilterMT.FilterMT{
      input:
        
    }

    output {
        File rna_featurecount_count = count.rna_featurecount_counts
        File rna_featurecount_summary = count.rna_featurecount_summary
    }
}
