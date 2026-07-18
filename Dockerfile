# ==========================================
# 1. Build Stage
# ==========================================
FROM elixir:1.20-otp-29-alpine AS builder

# Build dependencies
RUN apk add --no-cache \
    build-base \
    git \
    cmake \
    bash

WORKDIR /app

ENV MIX_ENV=prod

# Install Mix tooling
RUN mix local.hex --force && \
    mix local.rebar --force

# Install dependencies
COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV
RUN mix deps.compile

# Copy source
COPY config config/
COPY lib lib/

# Build release
RUN mix compile
RUN mix release

# ==========================================
# 2. Runtime Stage
# ==========================================
FROM alpine:3.24

RUN addgroup -S prism && adduser -S -G prism -h /home/prism prism

RUN apk add --no-cache \
    bash \
    openssl \
    ncurses-libs \
    libstdc++ \
    ca-certificates

WORKDIR /app

ENV MIX_ENV=prod

COPY --from=builder --chown=prism:prism /app/_build/prod/rel/prism ./

USER prism

EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/ready || exit 1

CMD ["bin/prism", "start"]
