FROM python:3.12-alpine3.20 AS alpine-base
RUN apk update && \
    apk upgrade --no-cache
   
FROM alpine-base AS python-build
WORKDIR /app
COPY src/ .

RUN apk add --no-cache g++ make cmake libffi-dev openssl-dev libc-dev python3-dev py3-virtualenv geos-dev jemalloc-dev boost-dev boost-filesystem boost-system boost-regex snappy-dev glog-dev brotli-dev apache-arrow apache-arrow-dev && \
    python3 -m venv venv && \
    . venv/bin/activate && \
    python3 -m ensurepip && \
    echo "after pip install" && \
    pip install --upgrade pip && \
    pip install --upgrade setuptools && \
    pip install pyarrow==16.0.0 && \
    pip install -r requirements.txt
# RUN apk add py3-gunicorn
FROM alpine-base AS python-runtime
WORKDIR /app
RUN apk update && \
    apk upgrade --no-cache

RUN apk add --no-cache python3 libffi openssl libc6-compat py3-virtualenv geos jemalloc-dev boost-dev boost-filesystem boost-system boost-regex snappy-dev glog-dev brotli-dev apache-arrow apache-arrow-dev
COPY --from=python-build /app .

RUN python --version
COPY src/ .
COPY scripts/gunicorn-start.sh .
COPY ./certs /app/certs
RUN chmod 777 gunicorn-start.sh
ENV PYTHONUNBUFFERED=1
ENV PORT=5001
EXPOSE 5001/tcp
ENTRYPOINT ["/app/gunicorn-start.sh"]