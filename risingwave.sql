CREATE TABLE IF NOT EXISTS kafka_source (id bytea)
WITH (
   connector='kafka',
   topic='jiamin-project',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   scan.startup.mode='earliest',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
) FORMAT PLAIN ENCODE BYTES;

create function proto_header_decode(bytea) returns struct<
   stream VARCHAR,
   pan VARCHAR
>
AS proto_header_decode using link 'http://udf-server:8815';

create function proto_decode_snapshot(bytea) returns struct<
   ds BIGINT,
   instance VARCHAR,
   marketType VARCHAR,
   eventTime BIGINT,
   logTime BIGINT,
   totalValue DOUBLE PRECISION,
   cash DOUBLE PRECISION,
   frozenCash DOUBLE PRECISION,
   dailyPnl DOUBLE PRECISION,
   marketValue DOUBLE PRECISION,
   margin DOUBLE PRECISION,
   commission DOUBLE PRECISION,
   serverMargin DOUBLE PRECISION,
   serverCommision DOUBLE PRECISION
   >
As proto_decode_snapshot using link 'http://udf-server:8815';

create function proto_decode_trade(bytea) returns struct<
   ds BIGINT,
   instance VARCHAR,
   marketType VARCHAR,
   eventTime BIGINT,
   logTime BIGINT,
   orderRef VARCHAR,
   code VARCHAR,
   direction VARCHAR,
   offset VARCHAR,
   val DOUBLE PRECISION,
   price DOUBLE PRECISION,
   volume DOUBLE PRECISION
>
AS proto_decode_trade using link 'http://udf-server:8815';

create function proto_decode_elixir_instrument(bytea) returns struct<
   ds BIGINT,
   instance VARCHAR,
   marketType VARCHAR,
   eventTime BIGINT,
   logTime BIGINT,
   columns VARCHAR[],
   rows VARCHAR[]
>
AS proto_decode_elixir_instrument using link 'http://udf-server:8815';

create view tag as select id, (proto_header_decode(_rw_key)).stream as "stream" from kafka_source;

create materialized view account_snapshot as select proto_decode_snapshot(id) AS decoded_snapshot from tag where stream = 'account_snapshot';

CREATE MATERIALIZED VIEW account_snapshot_v AS
SELECT
  to_timestamp((decoded_snapshot).ds/1000) AS ds,
  (decoded_snapshot).instance AS instance,
  (decoded_snapshot).marketType AS market_type,
  to_timestamp((decoded_snapshot).eventTime/1000) AS event_time,
  to_timestamp((decoded_snapshot).logTime/1000000) AS log_time,
  (decoded_snapshot).totalValue AS total_value,
  (decoded_snapshot).cash AS cash,
  (decoded_snapshot).frozenCash AS frozen_cash,
  (decoded_snapshot).dailyPnl AS daily_pnl,
  (decoded_snapshot).marketValue AS market_value,
  (decoded_snapshot).margin AS margin
FROM
  (
    SELECT
      proto_decode_snapshot(id) AS decoded_snapshot
    FROM
      tag
    WHERE stream = 'account_snapshot'
  ) AS decoded_snapshot
  ORDER BY ds, instance, market_type, event_time, log_time;

CREATE MATERIALIZED VIEW trade_v AS
SELECT
   to_timestamp((decoded_trade).ds/1000) AS ds,
  (decoded_trade).instance AS instance,
  (decoded_trade).marketType AS market_type,
  to_timestamp((decoded_trade).eventTime/1000) AS event_time,
  to_timestamp((decoded_trade).logTime/1000000) AS log_time,
  (decoded_trade).orderRef AS order_ref,
  (decoded_trade).code AS code,
  (decoded_trade).direction AS direction,
  (decoded_trade).val AS value,
  (decoded_trade).price AS price,
  (decoded_trade).volume AS volume
FROM
  (
    SELECT
      proto_decode_trade(id) AS decoded_trade
    FROM
      tag
    WHERE stream = 'trade'
  ) AS decoded_trade
  ORDER BY ds, instance, market_type, event_time, log_time;

CREATE MATERIALIZED VIEW elixir_instrument_v AS
SELECT
   to_timestamp((decoded_elixir).ds/1000) AS ds,
   (decoded_elixir).instance AS instance,
   (decoded_elixir).marketType AS market_type,
   to_timestamp((decoded_elixir).eventTime/1000) AS event_time,
   to_timestamp((decoded_elixir).logTime/1000000) AS log_time,
   unnest((decoded_elixir).columns)
FROM decoded_elixirs
   (decoded_elixir).target_position AS target_position,
   (decoded_elixir).real_position AS real_position,
   (decoded_elixir).market_value AS market_value,
   (decoded_elixir).target_market_value AS target_market_value,
   (decoded_elixir).server_position AS server_position
FROM 

CREATE MATERIALIZED VIEW cash_decrease_1_min_mv AS
SELECT
   window_start as time,
   window_end,
   instance,
   market_type,
   avg(cash) as cash,
   avg(total_value) as total_value
FROM tumble(account_snapshot_v, event_time, INTERVAL '1 MINUTE')
GROUP BY time, window_end, instance, market_type
ORDER BY time ASC,
         instance ASC;

SELECT time, sum(cash)/sum(total_value) from cash_decrease_1_min_mv;


CREATE MATERIALIZED VIEW pnl_decrease_1_min_mv AS
SELECT
   window_start as time,
   instance,
   avg(dailyPnl) as dailyPnl,
   avg(total_value) as total_value,
   sum(total_value) as sum_total_value,   
FROM tumble(account_snapshot_v, event_time, INTERVAL '1 MINUTE')
GROUP BY time, instance
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW traded_volume_15_min_mv AS
SELECT
   window_start as time,
   instance,
   sum(value) as value
FROM tumble(trade, event_time, INTERVAL '15 MINUTES')
GROUP BY time,
         instance
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW traded_volume_30_min_mv AS
SELECT
   window_start as time,
   instance,
   sum(value) as value
FROM tumble(trade, event_time, INTERVAL '30 MINUTES')
GROUP BY time,
         instance
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW traded_volume_per_direction_15min AS
SELECT
   window_start as time,
   instance,
   direction,
   sum(value) as value 
FROM tumble(trade_v, event_time, INTERVAL '15 MINUTES')
GROUP BY time,
         instance,
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW market_value_change AS
SELECT
   event_time as time,
   instance,
   market_value,
   avg(market_value) OVER (
      PARTITION BY instance
      ORDER BY time ASC
      Rows BETWEEN 15 PRECEDING AND CURRENT ROW
   ) AS moving_avg_15min
FROM account_snapshot_v
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW market_val_1_min_mv AS
SELECT
   window_start as time,
   instance,
   sum(market_value) as market_value
FROM tumble(elixir_instrument_v, event_time, INTERVAL '1 MINUTE')
GROUP BY time,
         instance
ORDER BY time ASC,
         instance ASC;

CREATE MATERIALIZED VIEW position_diff_1min_mv AS
SELECT
   window_start as time,
   instance,
   avg(real_postion) as real_postion,
   avg(target_position) as target_position
FROM tumble(elixir_instrument_v, event_time, INTERVAL '1 MINUTE')
GROUP BY time,
         instance
ORDER BY time ASC,
         instance ASC;

create materialized view trade as select proto_decode_trade(id) from tag where stream = 'trade';

create materialized view elixir_instrument as select proto_decode_elixir_instrument(id) from tag where stream = 'elixir_instrument';

select proto_decode_snapshot(id) from tag where stream = 'account_snapshot' limit 10;
select proto_decode_trade(id) from tag where stream = 'trade' limit 10;
