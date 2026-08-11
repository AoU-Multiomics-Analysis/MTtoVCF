#!/usr/bin/env bash
set -euo pipefail

vcf="$1"
index_destination="$2"
index_name="$(basename "${vcf}").tbi"

bcftools index --tbi --force --output "${index_name}" "${vcf}"
test -s "${index_name}"
gcloud storage cp "${index_name}" "${index_destination}"
printf '%s' "${index_destination}" > index_outpath.txt
