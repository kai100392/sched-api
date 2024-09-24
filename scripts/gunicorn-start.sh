#!/usr/bin/env sh

if [[ -z "${GUNICORN_APP_ROOT}" ]]; then
    GUNICORN_APP_ROOT=/app
fi

if [[ -z "${PORT}" ]]; then
    PORT=5001
fi

if [[ -z "${TIMEOUT}" ]]; then
    TIMEOUT=20
fi

. /app/venv/bin/activate

gunicorn --chdir ${GUNICORN_APP_ROOT} 'server:app' --workers 1 --worker-class uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT} --timeout ${TIMEOUT}