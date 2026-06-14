FROM public.ecr.aws/amazonlinux/amazonlinux:2023

RUN dnf install -y --allowerasing git curl python3 python3-pip unzip && dnf clean all
RUN useradd -m -d /home/agent -u 1000 agent

# Install kiro-cli (arm64 required by AgentCore)
USER agent
ENV HOME=/home/agent
RUN curl -fsSL https://cli.kiro.dev/install | bash
USER root

RUN pip3 install --no-cache-dir boto3

COPY --chown=agent:agent healthcheck.py /app/healthcheck.py
COPY --chown=agent:agent run.sh /app/run.sh
RUN chmod +x /app/run.sh

# Steering files (optional — customize for your use case)
RUN mkdir -p /home/agent/.kiro/steering && chown -R agent:agent /home/agent/.kiro
COPY --chown=agent:agent steering/ /home/agent/.kiro/steering/

RUN git config --global user.name "AgentCore Bot" && \
    git config --global user.email "agentcore@example.com" && \
    git config --global --add safe.directory '*'

ENV PATH="/home/agent/.local/bin:${PATH}"
WORKDIR /app
EXPOSE 8080
USER agent

CMD ["python3", "/app/healthcheck.py"]
