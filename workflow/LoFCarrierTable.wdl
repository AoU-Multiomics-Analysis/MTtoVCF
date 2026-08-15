version 1.0

workflow LoFCarrierTable {
    input {
        File vcf_file
        File vcf_index
        File transcript_annotations_tsv
        String output_prefix
        Int threads = 4
        String task_memory = "32G"
        String task_disk = "local-disk 500 SSD"
    }

    call ExtractLoFCarriers {
        input:
            vcf_file = vcf_file,
            vcf_index = vcf_index,
            transcript_annotations_tsv = transcript_annotations_tsv,
            output_prefix = output_prefix,
            threads = threads,
            task_memory = task_memory,
            task_disk = task_disk
    }

    output {
        File LoFCarriersHC = ExtractLoFCarriers.lof_carriers_hc
        File LoFCarriersHCOrLC = ExtractLoFCarriers.lof_carriers_hc_or_lc
        File SpliceAcceptorCarriers = ExtractLoFCarriers.splice_acceptor_carriers
        File SpliceDonorCarriers = ExtractLoFCarriers.splice_donor_carriers
    }
}

task ExtractLoFCarriers {
    input {
        File vcf_file
        File vcf_index
        File transcript_annotations_tsv
        String output_prefix
        Int threads = 4
        String task_memory = "32G"
        String task_disk = "local-disk 500 SSD"
    }

    command <<<
        set -euo pipefail

        if [ "~{vcf_index}" != "~{vcf_file}.tbi" ]; then
            ln -sf "~{vcf_index}" "~{vcf_file}.tbi"
        fi

        python3 /extract_lof_carriers.py write-sites \
            --TranscriptAnnotations "~{transcript_annotations_tsv}" \
            --Regions annotation_regions.tsv \
            --VariantMap annotation_variant_gene_map.tsv

        if [ -s annotation_regions.tsv ]; then
            bcftools view --threads ~{threads} \
                -R annotation_regions.tsv \
                "~{vcf_file}" \
                -Ov \
                -o lof_variants.vcf
        else
            touch lof_variants.vcf
        fi

        python3 /extract_lof_carriers.py collect-carriers \
            --VCF lof_variants.vcf \
            --VariantMap annotation_variant_gene_map.tsv \
            --OutputPrefix "~{output_prefix}"
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/lof-carriers:main"
        memory: task_memory
        cpu: threads
        disks: task_disk
    }

    output {
        File lof_carriers_hc = "~{output_prefix}.lof_carriers.HC.tsv.gz"
        File lof_carriers_hc_or_lc = "~{output_prefix}.lof_carriers.HC_or_LC.tsv.gz"
        File splice_acceptor_carriers = "~{output_prefix}.splice_acceptor_carriers.tsv.gz"
        File splice_donor_carriers = "~{output_prefix}.splice_donor_carriers.tsv.gz"
    }
}
