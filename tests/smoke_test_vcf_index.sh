#!/usr/bin/env bash
set -euo pipefail

image="${1:-mttovcf-utils:index-test}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

mkdir -p "${work_dir}/published" "${work_dir}/task"
cp "${repo_root}/tests/fixtures/tiny.vcf" "${work_dir}/tiny.vcf"
cp "${repo_root}/tests/fake_gcloud.sh" "${work_dir}/fake_gcloud"
chmod +x "${work_dir}/fake_gcloud"

docker run --rm \
    -v "${work_dir}:/data" \
    -v "${work_dir}/fake_gcloud:/opt/conda/bin/gcloud:ro" \
    "${image}" bash -c '
        set -euo pipefail
        bgzip --stdout /data/tiny.vcf > /data/tiny.vcf.bgz
        cd /data/task
        /index_vcf.sh /data/tiny.vcf.bgz /data/published/tiny.vcf.bgz.tbi
        test "$(cat index_outpath.txt)" = "/data/published/tiny.vcf.bgz.tbi"
        ln -s /data/tiny.vcf.bgz tiny.vcf.bgz
        rm -f tiny.vcf.bgz.tbi
        ln -s /data/published/tiny.vcf.bgz.tbi tiny.vcf.bgz.tbi
        bcftools view --no-header --regions chr1:100-100 tiny.vcf.bgz |
            grep -q "^chr1[[:space:]]100[[:space:]]"
    '
