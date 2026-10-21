# syntax=docker/dockerfile:1
# connector-qa-runner — hermetic test image.
#
# The suite is fully offline: all HTTP traffic runs through in-process
# httpx.MockTransport / ASGITransport handlers and conftest blocks outbound
# sockets, so the default command runs pytest with `--network=none`.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first so source edits do not invalidate the pip layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code, schemas, configs and the test suite.
COPY . .

# Default command: run the full test suite without network access.
CMD ["pytest", "test_suite", "-q", "--network=none"]
