FROM docker:29-cli AS docker-cli
FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
WORKDIR /opt/pisces
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
RUN python -m pip install --no-cache-dir ".[web,yaml]" \
    && groupadd --gid 50000 pisces \
    && useradd --uid 50000 --gid 50000 --home-dir /tmp --no-create-home pisces \
    && mkdir -p /var/lib/pisces && chown 50000:50000 /var/lib/pisces && chmod 2770 /var/lib/pisces
USER 50000:50000
CMD ["python", "-m", "uvicorn", "pisces.web:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM base AS executor
LABEL io.pisces.sandbox="1"
ENTRYPOINT ["/usr/local/bin/python"]
CMD ["-c", "print('PISCES execution image available')"]

FROM base AS worker
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
USER 0:50000
ENV DOCKER_HOST=unix:///var/run/docker.sock
CMD ["python", "-m", "pisces.service.worker"]

FROM base AS test
ENV PYTEST_ADDOPTS="-o cache_dir=/tmp/pytest-cache"
USER root
RUN python -m pip install --no-cache-dir ".[dev]" httpx
COPY tests/ ./tests/
COPY benchmarks/ ./benchmarks/
COPY frontend/src/test/fixtures/ ./frontend/src/test/fixtures/
COPY scripts/ ./scripts/
COPY docs/ ./docs/
COPY assets/ ./assets/
COPY demo/ ./demo/
COPY test_scripts/ ./test_scripts/
COPY plan.md ./plan.md
USER 50000:50000
CMD ["python", "-m", "pytest", "-q"]
