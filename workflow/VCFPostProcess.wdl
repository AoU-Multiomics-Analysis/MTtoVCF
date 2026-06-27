version 1.0

workflow VCFPostProcess {
    input {
        File vcf_file
        String output_prefix
        Boolean make_dosage = false
        Boolean make_plink = false
        Int dosage_threads = 4
        Int plink_new_id_max_allele_len = 200
    }

    if (make_dosage) {
        call BcftoolsDosage {
            input:
                vcf_file = vcf_file,
                output_prefix = output_prefix,
                threads = dosage_threads
        }
    }

    if (make_plink) {
        call Plink2 {
            input:
                vcf_file = vcf_file,
                output_prefix = output_prefix,
                new_id_max_allele_len = plink_new_id_max_allele_len
        }
    }

    output {
        File? GenotypeDosage = BcftoolsDosage.dosage
        File? GenotypeDosageIndex = BcftoolsDosage.dosage_index
        File? PlinkPgen = Plink2.pgen
        File? PlinkPvar = Plink2.pvar
        File? PlinkPsam = Plink2.psam
    }
}

task BcftoolsDosage {
    input {
        File vcf_file
        String output_prefix
        Int threads
    }

    command <<<
        set -euo pipefail

        printf 'CHROM\nPOS\nREF\nALT\n' > 4_columns.tsv
        bcftools query -l "~{vcf_file}" > sample_list.tsv
        cat 4_columns.tsv sample_list.tsv > header.tsv
        csvtk transpose header.tsv -T | gzip > header_row.tsv.gz

        bcftools +dosage --threads ~{threads} "~{vcf_file}" -- -t GT | tail -n+2 | gzip > dose_matrix.tsv.gz
        zcat header_row.tsv.gz dose_matrix.tsv.gz | bgzip > "~{output_prefix}.dose.tsv.gz"
        tabix -s1 -b2 -e2 -S1 "~{output_prefix}.dose.tsv.gz"
    >>>

    runtime {
        docker: "quay.io/eqtlcatalogue/susie-finemapping:v20.08.1"
        memory: "32G"
        cpu: threads
        disks: "local-disk 500 SSD"
    }

    output {
        File dosage = "~{output_prefix}.dose.tsv.gz"
        File dosage_index = "~{output_prefix}.dose.tsv.gz.tbi"
    }
}

task Plink2 {
    input {
        File vcf_file
        String output_prefix
        Int new_id_max_allele_len
    }

    command <<<
        set -euo pipefail

        plink2 --vcf "~{vcf_file}" \
            --make-pgen \
            --out "~{output_prefix}" \
            --set-all-var-ids @:#_\$r_\$a \
            --new-id-max-allele-len "~{new_id_max_allele_len}" \
            --output-chr chrM \
            --chr 1-22
    >>>

    runtime {
        docker: "quay.io/biocontainers/plink2:2.0.0a.6.9--h9948957_0"
        memory: "96G"
        cpu: 4
        disks: "local-disk 400 SSD"
    }

    output {
        File pgen = "~{output_prefix}.pgen"
        File pvar = "~{output_prefix}.pvar"
        File psam = "~{output_prefix}.psam"
    }
}
