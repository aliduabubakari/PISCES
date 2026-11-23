FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df

WORKDIR /opt/pisces
LABEL io.pisces.sandbox="1"
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
RUN python -m pip install --no-cache-dir \
    "apache-airflow==3.4.0" "apache-airflow-providers-standard==1.21.0" . \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt \
    && python -m pip check
RUN groupadd --gid 50000 pisces && useradd --uid 50000 --gid 50000 --home-dir /tmp --no-create-home pisces

USER 50000:50000
ENTRYPOINT ["/usr/local/bin/python", "-I", "-m", "pisces.energy.oci_worker"]
