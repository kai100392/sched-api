FROM python:3.12-slim AS python-base

WORKDIR /app
COPY src/ .

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libgeos-dev

# Set up virtual environment and install Python packages
RUN python3 -m venv venv && \
    . venv/bin/activate && \
    python3 -m ensurepip && \
    pip install --upgrade setuptools~=71.1 && \
    pip install pyarrow==17.0.0 && \
    pip install -r requirements.txt

COPY scripts/gunicorn-start.sh .
RUN chmod 777 gunicorn-start.sh

ENV PYTHONUNBUFFERED=1
ENV PORT=5001
EXPOSE 5001/tcp
ENTRYPOINT ["/app/gunicorn-start.sh"]
