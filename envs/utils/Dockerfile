FROM mambaorg/micromamba:1.5.3

## Set up environment
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
## Create a new environment and install packages
RUN micromamba install -y -n base -c conda-forge -c bioconda \
    conda-forge::r-tidyverse \
    conda-forge::r-data.table \
    conda-forge::r-optparse \
    conda-forge::r-arrow \
    conda-forge::r-r.utils \
    conda-forge::r-janitor \
    conda-forge::r-magrittr \
    conda-forge::r-arrow \
    bioconda::bcftools \
    bioconda::tabix 

COPY scripts/*  . 
