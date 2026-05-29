# Build stage
FROM elixir:1.18-alpine AS builder

# Install build dependencies
RUN apk add --no-cache build-base git

WORKDIR /app

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Set build ENV
ENV MIX_ENV=prod

# Install mix dependencies
COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV
RUN mix deps.compile

# Copy application code
COPY lib lib/
COPY config config/

# Compile the project and build the release
RUN mix compile
RUN mix release

# Release stage
FROM alpine:3.21 AS app

# Install runtime dependencies (Alpine needs ncurses, libstdc++, libgcc for Erlang)
RUN apk add --no-cache libstdc++ ncurses-libs bash openssl

WORKDIR /app

# Set runtime ENV
ENV MIX_ENV=prod

# Copy the compiled release from the builder stage
COPY --from=builder /app/_build/prod/rel/prism ./

# Set the default command to start the release
CMD ["bin/prism", "start"]
