FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        curl \
        unzip \
        samtools \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Only install runtime dependencies. Application source is mounted at runtime.
# All files are relative to the docker/ build context.
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip \
    && python -m pip install -r /tmp/requirements.txt

COPY entrypoint.sh /usr/local/bin/ngm-entrypoint
RUN chmod +x /usr/local/bin/ngm-entrypoint

ENTRYPOINT ["/usr/bin/tini", "--", "ngm-entrypoint"]
