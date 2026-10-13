# syntax=docker/dockerfile:1
# The robo-worker image: the service, claude, llmenv, gh, git, and kubectl.
# It holds no secret, no host path, and no docker socket. Only CI builds it.
# The cluster nodes are x86_64, so every binary below is the linux amd64 build.
ARG UV_DIGEST=sha256:cdc6093146eb3ff6a40107b38f008b789e050e77ad87865e381d9917da55a168
FROM ghcr.io/astral-sh/uv:0.13.0@${UV_DIGEST} AS uv

FROM python:3.14-slim-trixie@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170

ARG CLAUDE_CODE_VERSION=2.1.296
# The linux-x64 checksum from <claude releases>/<version>/manifest.json, pinned here so that a
# changed manifest cannot change the build.
ARG CLAUDE_CODE_SHA256=24972e3bc859fab2b46ed4c1e51f7d6130f06d3bd550811a114640de3370d0de
ARG LLMENV_VERSION=v3.12.2
ARG LLMENV_SHA256=d96bae9e6aeb9e989ba6a028973e1dc4afb5ecc6b2c8865a3cd2e1de65b524e0
ARG GH_VERSION=2.102.0
ARG GH_SHA256=bb766f710eef8ede859c18578c72c327597cd4c8a85b06001b1f3843c6019386
ARG KUBECTL_VERSION=v1.35.9
ARG KUBECTL_SHA256=3cfeaf80be482b435b0aa214aff6e0b2c312ee23c0ff20810c75517b6004c6eb

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# HOME is the build home. The manifests set HOME=/state/home at run time.
# The root filesystem is read-only at run time and the version is pinned, so no auto-update.
ENV HOME=/home/worker \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:${PATH} \
    DISABLE_AUTOUPDATER=1 \
    GH_CONFIG_DIR=/etc/robo-worker/gh \
    GIT_CONFIG_GLOBAL=/etc/robo-worker/git/gitconfig \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git jq openssh-client \
    && rm -rf /var/lib/apt/lists/*

# The worker is non-root. The mount points exist in the image, because the root filesystem
# is read-only at run time.
RUN useradd --create-home --uid 10001 --user-group worker \
    && mkdir -p /app /work /state /run/robo-worker/token \
        /etc/robo-worker/config /etc/robo-worker/secrets /etc/robo-worker/gh /etc/robo-worker/git \
    && chown worker:worker /app /work /state

RUN curl -fsSLo /usr/local/bin/kubectl \
        "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
    && echo "${KUBECTL_SHA256}  /usr/local/bin/kubectl" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/kubectl

ARG GH_URL=https://github.com/cli/cli/releases/download
RUN curl -fsSLo /tmp/gh.tar.gz "${GH_URL}/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz" \
    && echo "${GH_SHA256}  /tmp/gh.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/gh.tar.gz -C /tmp \
    && install -m 0755 "/tmp/gh_${GH_VERSION}_linux_amd64/bin/gh" /usr/local/bin/gh \
    && rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_amd64"

ARG LLMENV_URL=https://github.com/phaedrus1992/llmenv/releases/download
RUN curl -fsSLo /usr/local/bin/llmenv "${LLMENV_URL}/${LLMENV_VERSION}/llmenv-linux-x86_64" \
    && echo "${LLMENV_SHA256}  /usr/local/bin/llmenv" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/llmenv

# git asks gh for the GitHub credential. The gh Secret holds hosts.yml only.
RUN git config --system credential."https://github.com".helper "!gh auth git-credential"

# uv 0.13.0, pinned by digest in the first stage.
COPY --from=uv /uv /uvx /usr/local/bin/

# The Claude Code binary itself, from the same place as the native installer, without
# running the installer script.
ARG CLAUDE_URL=https://downloads.claude.ai/claude-code-releases
RUN curl -fsSLo /usr/local/bin/claude "${CLAUDE_URL}/${CLAUDE_CODE_VERSION}/linux-x64/claude" \
    && echo "${CLAUDE_CODE_SHA256}  /usr/local/bin/claude" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/claude \
    && claude --version

USER 10001:10001

WORKDIR /app
COPY --chown=worker:worker pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY --chown=worker:worker README.md ./
COPY --chown=worker:worker src ./src
RUN uv sync --frozen --no-dev

LABEL org.opencontainers.image.source="https://github.com/phaedrus1992/robo-worker" \
      org.opencontainers.image.description="claude -p jobs on repo checkouts, over MCP"

ENTRYPOINT ["/app/.venv/bin/robo-worker"]
CMD ["serve", "--config", "/etc/robo-worker/config/config.toml"]
