FROM mambaorg/micromamba:1.5.3

ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN micromamba install -y -n base -c conda-forge -c bioconda \
    conda-forge::python=3.11 \
    bioconda::bcftools \
    bioconda::htslib \
    && micromamba clean -a -y

USER root
RUN ln -sf /opt/conda/bin/python /usr/local/bin/python \
    && ln -sf /opt/conda/bin/python3 /usr/local/bin/python3 \
    && ln -sf /opt/conda/bin/bcftools /usr/local/bin/bcftools \
    && ln -sf /opt/conda/bin/bgzip /usr/local/bin/bgzip \
    && ln -sf /opt/conda/bin/tabix /usr/local/bin/tabix
USER mambauser

COPY scripts/extract_lof_carriers.py /extract_lof_carriers.py

CMD ["bash"]
