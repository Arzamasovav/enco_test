FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /opt/app

COPY pyproject.toml uv.lock ./

RUN uv sync --active --frozen --no-cache --no-install-project
