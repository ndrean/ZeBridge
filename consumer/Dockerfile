FROM elixir:1.17-otp-27-alpine AS builder

RUN apk add --no-cache build-base git

WORKDIR /app

ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force
RUN mix deps.get --only prod
RUN mix deps.compile

COPY config config/
COPY lib lib/

RUN mix compile

FROM elixir:1.17-otp-27-alpine

RUN apk add --no-cache libstdc++ openssl ncurses-libs

WORKDIR /app

COPY --from=builder /app/_build /app/_build
COPY --from=builder /app/deps /app/deps
COPY --from=builder /app/lib /app/lib
COPY --from=builder /app/mix.exs /app/mix.exs
COPY --from=builder /app/mix.lock /app/mix.lock
COPY --from=builder /app/config /app/config

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod

CMD ["mix", "run", "--no-halt"]
