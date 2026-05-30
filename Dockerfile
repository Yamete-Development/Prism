# Build stage
FROM elixir:1.19.5-alpine AS builder

RUN apk add --no-cache build-base git

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV
RUN mix deps.compile

COPY lib lib/
COPY config config/

RUN mix compile
RUN mix release

# Runtime stage
FROM elixir:1.19.5-alpine AS app

RUN apk add --no-cache \
    libstdc++ \
    ncurses-libs \
    bash \
    openssl

WORKDIR /app

ENV MIX_ENV=prod

COPY --from=builder /app/_build/prod/rel/prism ./

CMD ["bin/prism", "start"]
