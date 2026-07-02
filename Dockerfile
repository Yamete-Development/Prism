# ==========================================
# 1. Build Stage
# ==========================================
FROM elixir:1.19.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache build-base git cmake bash

WORKDIR /app

# Install package managers
RUN mix local.hex --force && \
    mix local.rebar --force

ENV MIX_ENV=prod

# Cache dependencies layer
COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV
RUN mix deps.compile

# Copy application code and config
COPY config config/
COPY lib lib/

# Compile and assemble the self-contained release
RUN mix compile
RUN mix release

# ==========================================
# 2. Runtime Stage (Optimized)
# ==========================================
# Instead of full Elixir, we use a bare alpine image
FROM alpine:3.20 AS app

# Install only the runtime OS libraries needed by the BEAM
RUN apk add --no-cache \
    libstdc++ \
    ncurses-libs \
    bash \
    openssl \
    ca-certificates

WORKDIR /app

ENV MIX_ENV=prod

# Copy the self-contained release from the builder stage
COPY --from=builder /app/_build/prod/rel/prism ./

CMD ["bin/prism", "start"]