FROM ghcr.io/arroyosystems/arroyo:latest

COPY pipeline.sql /opt/arroyo/pipeline.sql
COPY init.sh /opt/arroyo/init.sh

RUN chmod +x /opt/arroyo/init.sh && mkdir -p /var/lib/arroyo/checkpoints /var/lib/arroyo/artifacts

ENV ARROYO__DISABLE_TELEMETRY=true
