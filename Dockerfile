FROM python:3.11-slim as base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

RUN apt-get update
RUN apt-get install -y git

WORKDIR /app

FROM base as builder

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.6.1

RUN pip install "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock README.md ./
COPY hakai_profile_qc ./hakai_profile_qc

RUN poetry install

FROM base as final

COPY --from=builder /app/.venv ./.venv
COPY --from=builder /app/dist .

RUN ./.venv/bin/pip install *.whl
CMD ["python", "hakai_profile_qc", "--test-suite"]
