#!/usr/bin/env bash
set -euo pipefail

annotations="$1"
index_destination="$2"
annotation_name="$(basename "${annotations}")"

ln -s "${annotations}" "${annotation_name}"
tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1 "${annotation_name}"
index_name="${annotation_name}.tbi"
test -s "${index_name}"
gcloud storage cp "${index_name}" "${index_destination}"
printf '%s' "${index_destination}" > index_outpath.txt
