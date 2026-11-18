FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /srv/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data && \
    adduser --system --uid 10001 appuser && \
    chown -R appuser /data /srv/app
USER appuser

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/python", "-m"]
CMD ["app.web"]
