CREATE TABLE instance_list AS
SELECT * FROM unnest(array[
    'goliath_guojun_hh3',
    'goliath_guolian_hh9',
    'goliath_guoxin_hh7',
    'goliath_haitong_hh4',
    'goliath_haitong_hh5',
    'goliath_haitong_hh6',
    'goliath_hengtai_hh10',
    'goliath_jiantou_hh7',
    'goliath_lianchu_hh12',
    'goliath_tianfeng_hh3',
    'goliath_yinhe_hh11',
    'goliath_zhaoshang_hh9',
    'goliath_zhongcai_hh3',
    'goliath_zhongcai_hh6',
    'goliath_zhongxin_hh8'
]) AS instance;

CREATE MATERIALIZED VIEW no_data AS
SELECT 1 as no_data
FROM (
    SELECT MAX(event_time) as proc_time FROM account_snapshot_v
    UNION
    SELECT MAX(event_time) as proc_time FROM trade_v
    UNION
    SELECT MAX(event_time) as proc_time FROM elixir_instrument_v
) AS combined_data
HAVING NOW() > MAX(combined_data.proc_time) + INTERVAL '1' minute 
    AND NOW() < MAX(combined_data.proc_time) + INTERVAL '5' minute;

CREATE MATERIALIZED VIEW cash_decrease_to_2_percent AS
SELECT
    'cash_decrease_to_2_percent' as alert_type,
    cash_decrease_query.time,
    instance_list.instance,
    cash_decrease_query.ratio,
    cash_decrease_query.in_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, sum(cash)/sum(total_value) as ratio, sum(cash)/sum(total_value) < 0.02 as in_alert
    FROM cash_decrease_1_min_mv
    WHERE time BETWEEN now() - INTERVAL '2 MINUTE' AND now() - INTERVAL '1 MINUTE'
    GROUP BY time, instance
    ORDER BY instance
    ) AS cash_decrease_query ON instance_list.instance=cash_decrease_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW pnl_decrease_to_minus_2_percent AS
SELECT
    'pnl_decrease_to_minus_2_percent' as alert_type,
    pnl_decrease_query.time as time,
    instance_list.instance,
    pnl_decrease_query.ratio,
    pnl_decrease_query.in_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, sum(daily_pnl)/sum(total_value) as ratio, sum(daily_pnl)/sum(total_value) < -0.02 as in_alert
    FROM pnl_decrease_1_min_mv
    WHERE time BETWEEN now() - INTERVAL '2 MINUTE' AND now() - INTERVAL '1 MINUTE'
    GROUP BY time, instance
    ORDER BY instance
    ) AS pnl_decrease_query ON instance_list.instance=pnl_decrease_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW maket_value_dramatic_change AS
SELECT
    'maket_value_dramatic_change' as alert_type,
    market_value_change_query.time as time,
    instance_list.instance,
    market_value_change_query.ratio,
    market_value_change_query.in_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, abs(market_value - moving_avg_15min)/moving_avg_15min as ratio, abs(market_value - moving_avg_15min)/moving_avg_15min > 5 as in_alert
    FROM market_value_change
    WHERE time BETWEEN now() - INTERVAL '2 MINUTE' AND now() - INTERVAL '1 MINUTE'
    ORDER BY instance
    ) AS market_value_change_query ON instance_list.instance=market_value_change_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW turn_over_lower_than_05percent AS
SELECT
    'turn_over_lower_than_05percent' as alert_type,
    turn_over_query.time as time,
    instance_list.instance,
    turn_over_query.ratio,
    turn_over_query.in_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        sum(t1.value)/sum(t2.market_value) as ratio,
        sum(t1.value)/sum(t2.market_value) < 0.005 as in_alert
        FROM traded_volume_15_min_mv as t1
        INNER JOIN market_val_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now() - INTERVAL'15 MINUTES'
            AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now() - INTERVAL'15 MINUTES'
        GROUP BY t1.time, t1.instance
        ORDER BY t1.time, t1.instance
    ) AS turn_over_query ON instance_list.instance=turn_over_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW turn_over_higher_than_6percent AS
SELECT
    'turn_over_higher_than_6percent' as alert_type,
    turn_over_query.time as time,
    instance_list.instance,
    turn_over_query.direction,
    turn_over_query.ratio,
    turn_over_query.in_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        t1.direction,
        sum(t1.value) as total_value,
        sum(t2.market_value) as market_value,
        sum(t1.value)/sum(t2.market_value) as ratio,
        sum(t1.value)/sum(t2.market_value) > 0.06 as in_alert
        FROM traded_volume_per_direction_15min as t1
        INNER JOIN market_val_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now() - INTERVAL'15 MINUTES'
            AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now() - INTERVAL'15 MINUTES'
        GROUP BY t1.time, t1.instance, t1.direction
        ORDER BY t1.time, t1.instance, t1.direction
    ) AS turn_over_query ON instance_list.instance=turn_over_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW postion_diff_higher_than_5percent AS
SELECT
    'postion_diff_higher_than_5percent' as alert_type,
    position_diff_query.time as time,
    instance_list.instance,
    position_diff_query.ratio,
    position_diff_query.in_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        (sum(t1.real_position)-sum(t1.target_position))/sum(t2.sum_total_value) as ratio,
        (sum(t1.real_position)-sum(t1.target_position))/sum(t2.sum_total_value) > 0.05 as in_alert
        FROM position_diff_1min_mv as t1
        INNER JOIN pnl_decrease_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL '2 MINUTE' AND now() - INTERVAL '1 MINUTE'
            AND t2.time BETWEEN now() - INTERVAL '2 MINUTE' AND now() - INTERVAL '1 MINUTE'
        GROUP BY t1.time, t1.instance
        ORDER BY t1.time, t1.instance
    ) AS position_diff_query ON instance_list.instance=position_diff_query.instance
WHERE (in_alert is NULL or in_alert is TRUE)
ORDER BY time, instance_list.instance;

CREATE SINK cash_decrease_to_2_percent_sink FROM cash_decrease_to_2_percent
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);

CREATE SINK pnl_decrease_to_minus_2_percent_sink FROM pnl_decrease_to_minus_2_percent
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);

CREATE SINK maket_value_dramatic_change_sink FROM maket_value_dramatic_change
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);

CREATE SINK turn_over_lower_than_05percent_sink FROM turn_over_lower_than_05percent
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);

CREATE SINK turn_over_higher_than_6percent_sink FROM turn_over_higher_than_6percent
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);

CREATE SINK postion_diff_higher_than_5percent_sink FROM postion_diff_higher_than_5percent
WITH (
   connector='kafka',
   topic='jiamin-project-output',
   properties.bootstrap.server='alikafka-post-cn-8ed2id9gs001-1-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-2-vpc.alikafka.aliyuncs.com:9094,alikafka-post-cn-8ed2id9gs001-3-vpc.alikafka.aliyuncs.com:9094',
   properties.sasl.mechanism='PLAIN',
   properties.security.protocol='SASL_PLAINTEXT',
   properties.sasl.username='jiamin_huang',
   properties.sasl.password='jiamin.huang@2023'
)
FORMAT PLAIN ENCODE JSON(
   force_append_only='true'
);