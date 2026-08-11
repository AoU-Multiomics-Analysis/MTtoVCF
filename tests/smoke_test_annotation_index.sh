#!/usr/bin/env bash
set -euo pipefail

image="${1:-mttovcf-utils:index-test}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

mkdir -p "${work_dir}/published" "${work_dir}/task"
cp "${repo_root}/tests/fixtures/tiny.annotations.tsv" "${work_dir}/tiny.annotations.tsv"
cp "${repo_root}/tests/fake_gcloud.sh" "${work_dir}/fake_gcloud"
chmod +x "${work_dir}/fake_gcloud"

docker run --rm \
    -v "${work_dir}:/data" \
    -v "${work_dir}/fake_gcloud:/opt/conda/bin/gcloud:ro" \
    "${image}" bash -c '
        set -euo pipefail
        bgzip --stdout /data/tiny.annotations.tsv > /data/tiny.annotations.tsv.bgz
        cd /data/task
        /index_annotations.sh /data/tiny.annotations.tsv.bgz /data/published/tiny.annotations.tsv.bgz.tbi
        test "$(cat index_outpath.txt)" = "/data/published/tiny.annotations.tsv.bgz.tbi"
        rm -f tiny.annotations.tsv.bgz.tbi
        ln -s /data/published/tiny.annotations.tsv.bgz.tbi tiny.annotations.tsv.bgz.tbi
        tabix tiny.annotations.tsv.bgz chr1:200-200 |
            grep -q "^chr1[[:space:]]200[[:space:]]"
    '
