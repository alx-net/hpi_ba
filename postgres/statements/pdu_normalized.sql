
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

        percentile_disc(0.1) within group (order by "ActivePower") as q1,
        percentile_disc(0.9) within group (order by "ActivePower") as q9,

        percentile_disc(0.5) within group (order by "ActivePower") as median
    from source
    group by device_id
),

metrics as (
    select * from source
    where time >= now() - interval '1h'
),

joined as (
    select * from limits left join metrics using (device_id)
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
    "ActivePower"/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as "ActivePower",
    q_low/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as q_low,
    q_high/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as q_high,
    q1/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as q1,
    q9/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as q9,
    median/coalesce(nullif(greatest(abs(q_low), abs(q_high)),0), 1) as median,
    q_high as scale,
    amount
from joined_counts

