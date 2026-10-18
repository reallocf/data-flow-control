FROM node:22-bookworm-slim
ARG CODEX_VERSION=0.163.0
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @openai/codex@${CODEX_VERSION}
COPY entrypoint.py proxy.py smoke.py submission.schema.json workflow.schema.json /opt/experiment/
RUN mkdir /work && chown node:node /work
USER node
WORKDIR /work
ENTRYPOINT ["python3", "/opt/experiment/entrypoint.py"]
