-- Databricks notebook source
-- * * *
--
-- WOMENS_HEALTH BASIC METRICS 
-- AND POST PARTUM 
-- 2021-04-22
-- This file shows basic metrics for womens_health data leading up to analysis of contraception use comparing patients with or without contraception counseling.  
-- All data are for
--   - Patientes with an encounter after 2019
--   - LOT 2 data only
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- INIT THE SESSION
--
-- * * *

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
--
-- DISTINCT PATIENTS OVER TIME (CHART)
--
-- * * *

select
  concat(year(coalesce(encounter_date)), '-', format_number(month(coalesce(encounter_date)),'00')) `Month`,  
  count(distinct patient_id) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- DISTINCT PATIENTS OVER TIME (TABLE)
--
-- * * *

select
  concat(year(coalesce(encounter_date)), '-', format_number(month(coalesce(encounter_date)),'00')) `Month`,  
  format_number(count(distinct patient_id),0) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- DISTINCT ENCOUNTERS OVER TIME (CHART)
--
-- * * *

select
  concat(year(coalesce(encounter_date)), '-', format_number(month(coalesce(encounter_date)),'00')) `Month`,  
  count(distinct encounter_id) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- DISTINCT ENCOUNTERS OVER TIME (TABLE)
--
-- * * *

select
  concat(year(coalesce(encounter_date)), '-', format_number(month(coalesce(encounter_date)),'00')) `Month`,  
  format_number(count(distinct encounter_id),0) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- PATIENTS BY ORG (CHART)
--
-- * * *

select
  org `Org`,
  count(distinct patient_id) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- PATIENTS BY ORG (TABLE)
--
-- * * *

select
  org `Org`,
  format_number(count(distinct patient_id),0) `Distinct Patients`
from
  enc
where 1=1
  and data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- Distinct Patients with Pregnancies by org (Chart)
--
-- * * *

select
  org `Org`,
  count(distinct patient_id) `Distinct Patients With Pregnancy`
from
  pregnancy
where 1=1
  and data_lot = 'LOT 2'
  and year(est_delivery_date) > 2019
group by 1
order by 1

-- COMMAND ----------

-- * * *
--
-- Distinct Patients with Pregnancies by org (Table)
--
-- * * *

select 
  'Total' `Org`, 
  format_number(count(distinct patient_id),0) `Distinct Patients With Pregnancy` 
from 
  pregnancy
where 1=1
  and data_lot = 'LOT 2'
  and year(est_delivery_date) > 2019
  
union all

select
  org `Org`,
  format_number(count(distinct patient_id),0) `Distinct Patients With Pregnancy`
from
  pregnancy
where 1=1
  and data_lot = 'LOT 2'
  and year(est_delivery_date) > 2019
group by 1
order by 1

-- COMMAND ----------

-- * * *
--
-- Range for Estimated Deliery Date (Chart)
--
-- * * *

select
  months_between_min_max as `Estimated Delivery Date Range (months)`,
  count(distinct patient_id) as `Number of Distinct Patients`
from (
  select
    patient_id,
    min(est_delivery_date) min_est_delivery_date,
    max(est_delivery_date) max_est_delivery_date,
    cast(months_between(max(est_delivery_date), min(est_delivery_date)) as INT) months_between_min_max
  from
    pregnancy
  where 1=1
    and data_lot = 'LOT 2'
    and year(est_delivery_date) > 2019
  group by 1
  order by 4 desc
)
group by 1
order by 1 asc
;

-- COMMAND ----------

-- * * *
--
-- Range for Estimated Deliery Date in MONTHS (Table)
--
-- * * *

select
  months_between_min_max as `Estimated Delivery Date Range (months)`,
  format_number(count(distinct patient_id),0) as `Number of Distinct Patients`
from (
  select
    patient_id,
    min(est_delivery_date) min_est_delivery_date,
    max(est_delivery_date) max_est_delivery_date,
    cast(months_between(max(est_delivery_date), min(est_delivery_date)) as INT) months_between_min_max
  from
    pregnancy
  where 1=1
    and data_lot = 'LOT 2'
    and year(est_delivery_date) > 2019
  group by 1
  order by 4 desc
)
group by 1
order by 1 asc
;

-- COMMAND ----------

-- * * *
--
-- POST PARTUM VISITS
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- PREGNANCIES WHERE MIN EXP DELIVERY = MIN EXP DELIVERY
--
-- * * *

select 'pregnancy' metric, count(distinct patient_id) from pregnancy
union all
select 'pp_pregnancy' metric, count(distinct patient_id) from pp_pregnancy
union all
select 'pct' metric, format_number((select (select count(distinct patient_id) from pp_pregnancy)/(select count(distinct patient_id) from pregnancy)) * 100, 2)
;

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM VISIT COUNTS (Chart)
-- 
-- * * *

select 
  weeks_after `Weeks Post Partum`,
  count(distinct patient_id) `Distinct Patients`,
  count(distinct patient_id)/(select count(distinct patient_id) from pregnancy) * 100 `Percent Total`
from 
  first_pp_visit
where
  months_after <= 6
group by 1
order by 1

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM VISIT PERCENT (Chart)
-- 
-- * * *

select 
  weeks_after `Weeks Post Partum`,
  count(distinct patient_id) `Distinct Patients`,
  count(distinct patient_id)/(select count(distinct patient_id) from pregnancy) * 100 `Percent Total`
from 
  first_pp_visit
where
  months_after <= 6
group by 1
order by 1

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM BY GROUPING
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- HOW DAYS/WEEKS AFTER MATH WORKS
--
-- * * *

select distinct
  days_after, 
  weeks_after
from
  first_pp_visit
where
  days_after < 30
order by days_after
;

-- COMMAND ----------

select distinct
  months_after,
  weeks_after
from
  first_pp_visit
where
  months_after < 6
order by 1,2


-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM BY GROUPING (Distinct Patients Chart)
--
-- * * *

select
  weeks_post_partum `First Post Partum Visit (weeks)`,
  sort_order,
  sum(distinct_patients) over (order by sort_order asc) `Distinct Patients`,
  sum(percent_total) over (order by sort_order asc) `Percent Total`
from (
  select 
    (case 
      when weeks_after < 2 then '2 Weeks'
      when weeks_after < 4 then '4 Weeks'
      when weeks_after < 8 then '8 Weeks'
      when weeks_after < 12 then '12 Weeks'
      when weeks_after < 24 then '24 Weeks (6 Months)'
      when weeks_after < 52 then '52 Weeks (1 Year)'
    end) weeks_post_partum,
    (case 
      when weeks_after < 2 then 1
      when weeks_after < 4 then 2
      when weeks_after < 8 then 3
      when weeks_after < 12 then 4
      when weeks_after < 24 then 5
      when weeks_after < 52 then 6
    end) sort_order,
    count(distinct patient_id) distinct_patients,
    count(distinct patient_id)/(select count(distinct patient_id) from pregnancy) * 100 percent_total
  from 
    first_pp_visit
  where
    months_after < 6
  group by 1,2
)

order by sort_order

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM BY GROUPING (Percent of Total Population Chart)
--
-- * * *

select
  weeks_post_partum `First Post Partum Visit (weeks)`,
  sort_order,
  sum(distinct_patients) over (order by sort_order asc) `Distinct Patients`,
  sum(percent_total) over (order by sort_order asc) `Percent Total`
from (
  select 
    (case 
      when weeks_after < 2 then '2 Weeks'
      when weeks_after < 4 then '4 Weeks'
      when weeks_after < 8 then '8 Weeks'
      when weeks_after < 12 then '12 Weeks'
      when weeks_after < 24 then '24 Weeks (6 Months)'
      when weeks_after < 52 then '52 Weeks (1 Year)'
    end) weeks_post_partum,
    (case 
      when weeks_after < 2 then 1
      when weeks_after < 4 then 2
      when weeks_after < 8 then 3
      when weeks_after < 12 then 4
      when weeks_after < 24 then 5
      when weeks_after < 52 then 6
    end) sort_order,
    count(distinct patient_id) distinct_patients,
    count(distinct patient_id)/(select count(distinct patient_id) from pregnancy) * 100 percent_total
  from 
    first_pp_visit
  where
    months_after < 6
  group by 1,2
)

order by sort_order

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM BY GROUPING (Table)
--
-- * * *

select
  weeks_post_partum `First Post Partum Visit (weeks)`,
  format_number(sum(distinct_patients) over (order by sort_order asc),0) `Distinct Patients`,
  format_number(sum(percent_total) over (order by sort_order asc),2) `Percent Total`
from (
  select 
    (case 
      when weeks_after < 2 then '2 Weeks'
      when weeks_after < 4 then '4 Weeks'
      when weeks_after < 8 then '8 Weeks'
      when weeks_after < 12 then '12 Weeks'
      when weeks_after < 24 then '24 Weeks (6 Months)'
      when weeks_after < 52 then '52 Weeks (1 Year)'
    end) weeks_post_partum,
    (case 
      when weeks_after < 2 then 1
      when weeks_after < 4 then 2
      when weeks_after < 8 then 3
      when weeks_after < 12 then 4
      when weeks_after < 24 then 5
      when weeks_after < 52 then 6
    end) sort_order,
    count(distinct patient_id) distinct_patients,
    count(distinct patient_id)/(select count(distinct patient_id) from pregnancy) * 100 percent_total
  from 
    first_pp_visit
  where
    months_after < 6
  group by 1,2
)

order by sort_order

-- COMMAND ----------

-- * * *
--
-- RACE
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- PREGNANCIES BY RACE (Table)
--
-- * * *

select
  coalesce(race_nachc, 'Other') `Race`,
  format_number(count(distinct preg.patient_id),0) `Distinct Patients`
from
  pregnancy preg
  left outer join patient_race_nachc race on race.patient_id = preg.patient_id
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- PREGNANCIES BY RACE (Chart)
--
-- * * *

select
  coalesce(race_nachc, 'Other') `Race`,
  count(distinct preg.patient_id) `Distinct Patients`
from
  pregnancy preg
  left outer join patient_race_nachc race on race.patient_id = preg.patient_id
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
--
-- FIRST POST PARTUM BY RACE (cumulative)
--
-- * * *
select
  race_nachc `Race`,
  weeks_post_partum `First Post Partum Visit (weeks)`,
  format_number(sum(distinct_patients) over (partition by race_nachc order by sort_order asc),0) `Distinct Patients`,
  format_number(sum(percent_total) over (partition by race_nachc order by sort_order asc),2) `Percent Total`
from (
   select 
    (case 
      when weeks_after < 2 then '2 Weeks'
      when weeks_after < 4 then '4 Weeks'
      when weeks_after < 8 then '8 Weeks'
      when weeks_after < 12 then '12 Weeks'
      when weeks_after < 24 then '24 Weeks (6 Months)'
      when weeks_after < 52 then '52 Weeks (1 Year)'
    end) weeks_post_partum,
    (case 
      when weeks_after < 2 then 1
      when weeks_after < 4 then 2
      when weeks_after < 8 then 3
      when weeks_after < 12 then 4
      when weeks_after < 24 then 5
      when weeks_after < 52 then 6
    end) sort_order,
    coalesce(race.race_nachc, 'Other') race_nachc,
    race_counts.distinct_patients total_patients,
    count(distinct pp.patient_id) distinct_patients,
    count(distinct pp.patient_id)/race_counts.distinct_patients * 100 percent_total
  from 
    first_pp_visit pp
    left outer join patient_race_nachc race on race.patient_id = pp.patient_id
    join (
      select
        coalesce(race.race_nachc, 'Other') race_nachc,
        count(distinct preg.patient_id) distinct_patients
      from
        pregnancy preg
        left outer join patient_race_nachc race on race.patient_id = preg.patient_id
      group by 1
  ) race_counts on race_counts.race_nachc = race.race_nachc
  where
    months_after < 6
  group by 1,2,3,4
  order by sort_order, race_nachc
)
order by sort_order, race_nachc
;
