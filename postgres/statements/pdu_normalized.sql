
with
source as (
    select
        to_timestamp(timestamp) as time,
        source as device_id,
        "snmp_activePowerL1"+"snmp_activePowerL2"+"snmp_activePowerL3" as "ActivePower"
    from metrics_external.pdu_tags join metrics_external.pdu_fields on hash = tags_hash
),

limits as (
    select
        device_id,

        percentile_disc(0.005) within group (order by "ActivePower") as q_low,
        percentile_disc(0.995) within group (order by "ActivePower") as q_high,

        percentile_disc(0.25) within group (order by "ActivePower") as q1,
        percentile_disc(0.75) within group (order by "ActivePower") as q3,

        percentile_disc(0.5) within group (order by "ActivePower") as median
    from source
    group by device_id
),

limits_enhanced as (
    select
        device_id,
        coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as scale,
        q3-q1 as iqr,
        q1,
        q3,
        median
    from limits
),

thresholds as (
    select
        device_id,
        scale,
        q1 - (1.5*iqr) as lower_threshold,
        q3 + (1.5*iqr) as higher_threshold,
        q1,
        q3,
        median
    from limits_enhanced
),

metrics as (
    select * from source
    where time >= now() - interval '1h'
),

joined as (
    select * from thresholds left join metrics using (device_id)
),

counts as (
    select device_id, count(*) as amount from joined group by device_id
),

joined_counts as (
    select * from joined left join counts using (device_id)
)
select
    time,
    device_id,
    "ActivePower"/scale as "ActivePower",
    q1/scale as q1,
    q3/scale as q3,
    lower_threshold/scale as lower_threshold,
    higher_threshold/scale as higher_threshold,
    median/scale as median,
    amount,
    scale
from joined_counts
