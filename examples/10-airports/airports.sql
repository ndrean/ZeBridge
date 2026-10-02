-- The world's airports: one public, read-only table, the same rows for every client.
-- Every text column is bounded, so no row can outgrow the bridge's buffer.
CREATE TABLE IF NOT EXISTS public.airports (
    code        varchar(3)  PRIMARY KEY,     -- IATA
    icao        varchar(4),
    name        varchar(80) NOT NULL,
    latitude    double precision NOT NULL,
    longitude   double precision NOT NULL,
    elevation   integer,                     -- feet
    url         varchar(160),
    time_zone   varchar(40),
    city_code   varchar(3),
    country     varchar(2),                  -- ISO 3166 alpha-2
    city        varchar(64),
    state       varchar(64),
    county      varchar(64),
    type        varchar(2),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

SELECT step, status FROM zebridge_enable('public.airports',
    public_reason => 'public airports list',
    publication   => 'my_pub',          -- your bridge's BRIDGE_CDC_PUBLICATION
    dry_run       => false);
