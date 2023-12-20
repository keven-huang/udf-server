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

CREATE TABLE alert_judge_time_line AS
SELECT
    range as time,
    CASE
        WHEN EXTRACT(HOUR FROM range) BETWEEN 9 AND 15 THEN true
        WHEN (EXTRACT(HOUR FROM range) BETWEEN 0 AND 8) OR (EXTRACT(HOUR FROM range) BETWEEN 16 AND 23) THEN false
        ELSE NULL
    END AS if_alert
FROM 
range(
    '2022-12-01 00:00:00'::TIMESTAMP,
    '2022-12-02 00:00:00'::TIMESTAMP, 
    interval '1' MINUTE
)
ORDER BY time;

CREATE MATERIALIZED VIEW cash_decrease_to_2_precent AS
SELECT
    cash_decrease_query.time,
    instance_list.instance,
    cash_decrease_query.ratio,
    cash_decrease_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, sum(cash)/sum(total_value) as ratio, sum(cash)/sum(total_value) < 0.02 as in_alert
    FROM cash_decrease_1_min_mv
    WHERE time BETWEEN now() - INTERVAL '1 MINUTE' AND now()
    GROUP BY time, instance
    ORDER BY instance
    ) AS cash_decrease_query ON instance_list.instance=cash_decrease_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(cash_decrease_query.time,'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW cash_decrease_to_2_precent AS
SELECT
    COALESCE(cash_decrease_query.time,now()) as time,
    instance_list.instance,
    cash_decrease_query.value,
    cash_decrease_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, sum(cash)/sum(total_value) as value, sum(cash)/sum(total_value) < 0.02 as in_alert
    FROM cash_decrease_1_min_mv
    WHERE time BETWEEN now() - INTERVAL '1 MINUTE' AND now()
    GROUP BY time, instance
    ORDER BY instance
    ) AS cash_decrease_query ON instance_list.instance=cash_decrease_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(cash_decrease_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;


CREATE MATERIALIZED VIEW pnl_decrease_5_percent_mv AS
SELECT time, instance, sum(daily_pnl)/sum(total_value) as result
FROM pnl_decrease_1_min_mv
WHERE time BETWEEN now() - INTERVAL '1 MINUTES' AND now()
GROUP BY time, instance
HAVING sum(daily_pnl)/sum(total_value) > 0
ORDER BY time, instance

SELECT
    COALESCE(pnl_decrease_query.time,now()) as time,
    instance_list.instance,
    pnl_decrease_query.value,
    pnl_decrease_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, sum(daily_pnl)/sum(total_value) as value, sum(daily_pnl)/sum(total_value) < 0.02 as in_alert
    FROM pnl_decrease_5_percent_mv
    WHERE time BETWEEN now() - INTERVAL '1 MINUTE' AND now()
    GROUP BY time, instance
    ORDER BY instance
    ) AS pnl_decrease_query ON instance_list.instance=pnl_decrease_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(pnl_decrease_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

SELECT
    COALESCE(market_value_change_query.time,now()) as time,
    instance_list.instance,
    market_value_change_query.value,
    market_value_change_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
    SELECT time, instance, abs(market_value - moving_avg_15min)/moving_avg_15min as value, sum(daily_pnl)/sum(total_value) < 0.02 as in_alert
    FROM pnl_decrease_5_percent_mv
    WHERE time BETWEEN now() - INTERVAL '1 MINUTE' AND now()
    GROUP BY time, instance
    ORDER BY instance
    ) AS market_value_change_query ON instance_list.instance=market_value_change_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(market_value_change_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW traded_volume AS
SELECT cash, total_value
FROM cash
WHERE time <= now() AND time >= now - INTERVAL '1 MINUTES'
GROUP BY time, instance
cash/total_value < 2
ORDER BY time, instance

CREATE MATERIALIZED VIEW mv AS
SELECT cash, total_value
FROM cash
WHERE time <= now() AND time >= now - INTERVAL '1 MINUTES' AND
cash/total_value < 2;

CREATE MATERIALIZED VIEW mv AS
SELECT 
t1.time,
t1.instance,
t1.direction,
sum(t1.value) as total_value,
sum(t2.market_value) as market_value,
sum(t1.value)/sum(t2.market_value) as value
sum(t1.value)/sum(t2.market_value) > 0.06 as in_alert,
FROM traded_volume_per_direction_15min as t1
INNER JOIN market_val_1_min_mv as t2
ON t1.time = t2.time AND t1.instance = t2.instance
WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
    AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
GROUP BY t1.time, t1.instance, t1.direction
ORDER BY t1.time, t1.instance, t1.direction;

SELECT
    COALESCE(turn_over_query.time,now()) as time,
    instance_list.instance,
    turn_over_query.value,
    turn_over_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        t1.direction,
        sum(t1.value) as total_value,
        sum(t2.market_value) as market_value,
        sum(t1.value)/sum(t2.market_value) as value,
        sum(t1.value)/sum(t2.market_value) > 0.06 as in_alert
        FROM traded_volume_15_min_mv as t1
        INNER JOIN market_val_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
            AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
        GROUP BY t1.time, t1.instance, t1.direction
        ORDER BY t1.time, t1.instance, t1.direction
    ) AS turn_over_query ON instance_list.instance=turn_over_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(turn_over_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

SELECT
    COALESCE(turn_over_query.time,now()) as time,
    instance_list.instance,
    turn_over_query.value,
    turn_over_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        sum(t1.value) as total_value,
        sum(t2.market_value) as market_value,
        sum(t1.value)/sum(t2.market_value) as value,
        sum(t1.value)/sum(t2.market_value) < 0.005 as in_alert
        FROM traded_volume_per_direction_15min as t1
        INNER JOIN market_val_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
            AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
        GROUP BY t1.time, t1.instance, t1.direction
        ORDER BY t1.time, t1.instance, t1.direction
    ) AS turn_over_query ON instance_list.instance=turn_over_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(turn_over_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

SELECT
    COALESCE(position_diff_query.time,now()) as time,
    instance_list.instance,
    position_diff_query.value,
    position_diff_query.in_alert,
    alert_judge_time_line.if_alert
FROM instance_list
LEFT JOIN
    (
        SELECT 
        t1.time,
        t1.instance,
        sum(real_position)-sum(target_position),
        sum(sum_total_value),
        (sum(real_position)-sum(target_position))/sum(sum_total_value) as value,
        (sum(real_position)-sum(target_position))/sum(sum_total_value) > 0 as in_alert
        FROM position_diff_1min_mv as t1
        INNER JOIN pnl_decrease_1_min_mv as t2
        ON t1.time = t2.time AND t1.instance = t2.instance
        WHERE t1.time BETWEEN now() - INTERVAL'1 MINUTE' AND now()
            AND t2.time BETWEEN now() - INTERVAL'1 MINUTE' AND now()
        GROUP BY t1.time, t1.instance
        ORDER BY t1.time, t1.instance
    ) AS position_diff_query ON instance_list.instance=position_diff_query.instance
INNER JOIN alert_judge_time_line ON 
to_char(alert_judge_time_line.time, 'HH24:MI')
=to_char(COALESCE(position_diff_query.time,now()),'HH24:MI')
WHERE (in_alert is NULL or in_alert is TRUE) AND if_alert is TRUE
ORDER BY time, instance_list.instance;

CREATE MATERIALIZED VIEW mv AS
SELECT 
    t1.time,
    t1.instance,
    t1.direction,
    sum(t1.value) as total_value,
    sum(t2.market_value) as market_value,
    sum(t1.value)/sum(t2.market_value)
FROM traded_volume_per_direction_15min as t1
INNER JOIN market_val_1_min_mv as t2
ON t1.time = t2.time AND t1.instance = t2.instance
WHERE t1.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
    AND t2.time BETWEEN now() - INTERVAL'30 MINUTES' AND now()
GROUP BY t1.time, t1.instance, t1.direction
HAVING sum(t1.value)/sum(t2.market_value) > 0.06
ORDER BY t1.time, t1.instance, t1.direction;

#error
market_value_change

CREATE SINK sink1 FROM mv1
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