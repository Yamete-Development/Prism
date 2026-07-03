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

RUN apk add --no-cache \
    bash \
    openssl \
    ncurses-libs \
    libstdc++ \
    ca-certificates

WORKDIR /app

ENV MIX_ENV=prod

COPY --from=builder /app/_build/prod/rel/prism ./

CMD ["bin/prism", "start"]
