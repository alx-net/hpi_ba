
with
limits as (
    select
        device_id,
        percentile_disc(0.005) within group (order by "ActivePower") as q_low,
        percentile_disc(0.995) within group (order by "ActivePower") as q_high,

        percentile_disc(0.1) within group (order by "ActivePower") as q1,
        percentile_disc(0.9) within group (order by "ActivePower") as q9,

        percentile_disc(0.5) within group (order by "ActivePower") as median
    from metrics.electricity_meters
    group by device_id
),

metrics as (
    select * from metrics.electricity_meters
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
    "ActivePower"/coalesce(nullif(median,0), 1) as "ActivePower",
    q_low/coalesce(nullif(median,0), 1) as q_low,
    q_high/coalesce(nullif(median,0), 1) as q_high,
    q1/coalesce(nullif(median,0), 1) as q1,
    q9/coalesce(nullif(median,0), 1) as q9,
    median/coalesce(nullif(median,0), 1) as median,
    median as scale,
    amount
from joined_counts

