-- Databricks notebook source
-- * * *
-- 
-- (2021-01-15)
-- 002-WOMENS-HEALTH-PP
--
-- Queries for post-partum project.  
--   - All queries in this notebook are limited to org in('ac, 'ochin') and data_lot = 'LOT 2'
--   - Includes queries for diab
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
-- Distinct patients by org
-- 
-- * * *
 
select distinct
  org.org,
  format_number(count (distinct enc.patient_id), 0) distinct_patient_count
from
  org
  left join enc on org.org = enc.org
where 1=1
  and year(encounter_date) > 2019
  and org.org in ('ac', 'ochin')
  and data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- Distinct encounters by org
-- 
-- * * *
 
select distinct
  concat('Total: ', org.org) org,
  format_number(count (distinct enc.encounter_id), 0) distinct_enc_count
from
  org
  left join enc on org.org = enc.org
where 1=1
  and year(encounter_date) > 2019
  and org.org in ('ac', 'ochin')
  and data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

-- * * * 
-- 
-- discrepancy in ac preg count is from pregnancies before 2020
-- 
-- * * *

-- COMMAND ----------

select
  preg.org,
  count(distinct preg.patient_id)
from 
  -- enc
  /*join*/ pregnancy preg /*on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019*/
where 1=1
--  and year(encounter_date) > 2019
  and preg.org in ('ac', 'ochin')
  and preg.data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

select
  enc.org,
  count(distinct enc.patient_id)
from 
  enc
  join pregnancy preg on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and enc.data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- Total pregnancies (derived)
-- 
-- * * *

select 
  enc.org `Org`,
  format_number(count(distinct preg.patient_id),0) `Distinct Patients With Pregnancy (Derived)`,
  format_number(count(distinct enc.patient_id),0) `Distinct Total Patients`,
  format_number(count(distinct preg.patient_id)/count(distinct enc.patient_id)*100,2) `Rate`
from 
  enc
  left outer join pregnancy preg on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and enc.data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- Total pregnancies by inclusion criteria
--
-- * * *
 
select
  preg.org,
  inclusion_reason,
  format_number(count(distinct preg.patient_id),0) distinct_patient_count
from 
  enc
  join pregnancy preg on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2
;

-- COMMAND ----------

-- * * *
--
-- Derived pregnancies v. confirmed pregnancies
-- (there's a big difference)
-- 
-- * * *
 
select 
  enc.org,
  'derived' as name,
  format_number(count(distinct preg.patient_id),0) distinct_patients
from 
  enc
  join pregnancy preg on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.org in ('ochin')
  and enc.data_lot = 'LOT 2'
group by 1,2
 
union all
select 
  enc.org, 
  'confirmed' as name, 
  format_number(count(distinct pat_id),0) distinct_patients 
from 
  enc
  join conf_pregnancy conf on enc.org_patient_id = conf.pat_id
where 1=1
  and year(encounter_date) >= 2019
  and enc.org in ('ochin')
  and enc.data_lot = 'LOT 2'
 group by 1,2
;

-- COMMAND ----------

-- * * *
-- 
-- ESTIMATED DELIVERY DATES
-- 
-- * * *
 
select 
  preg.org,
  preg.inclusion_reason,
  concat(year(preg.est_delivery_date), '-', format_number(month(preg.est_delivery_date), '00')) `Est Delivery Date (Month)`,
  count(distinct preg.patient_id) `Distinct Patient Count`
from 
  pregnancy preg
  join enc on preg.patient_id = enc.patient_id
where 1=1
  and year(preg.est_delivery_date) > 2019
  and preg.org in ('ac', 'ochin')
  and preg.data_lot = 'LOT 2'  
group by 1,2,3
order by 3,1,2
;

-- COMMAND ----------

-- * * *
-- 
-- POST-DELIVERY ENCOUNTERS 
-- (Based on estimated delivery date)
--
-- * * *

select * from (
select 
  preg.org as `Org`,
  case 
    when enc.encounter_date < date_add(preg.est_delivery_date, (7*2)) then '02 Weeks'
    when enc.encounter_date < date_add(preg.est_delivery_date, (7*8)) then '08 Weeks'
    when enc.encounter_date < date_add(preg.est_delivery_date, (7*17)) then '17 Weeks'
    else null
  end
  as `Time Box (months, not cumulative)`,
  count(distinct preg.patient_id) as `Distinct Patients`
from 
  pregnancy preg
  left outer join enc on preg.patient_id = enc.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.encounter_date > preg.est_delivery_date
  and enc.org in ('ac','ochin')
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 2,1
)
where  `Time Box (months, not cumulative)` is not null
;


-- COMMAND ----------

-- * * *
-- 
-- CUMULATIVE
-- POST-DELIVERY ENCOUNTERS 
-- (Based on estimated delivery date)
--
-- * * *

select
  org,
  count(distinct 02_weeks) as `02 Weeks`,
  count(distinct 08_weeks) as `08 Weeks`,
  count(distinct 17_weeks) as `17 Weeks`
from (
  select 
    preg.org,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*2)), preg.patient_id, null) as 02_weeks,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*8)), preg.patient_id, null) as 08_weeks,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*17)), preg.patient_id, null) as 17_weeks
  from 
    pregnancy preg
    left outer join enc on preg.patient_id = enc.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.encounter_date > preg.est_delivery_date
    and enc.org in ('ac','ochin')
    and enc.data_lot = 'LOT 2'
)
group by 1
order by 1


-- COMMAND ----------

select 
  org as `Org`,
  enc_count as `Number of Encounters`,
  count(distinct patient_id) as `Distinct Patient Count`
from (
  select distinct
    preg.org,
    preg.patient_id,
    count(distinct enc.encounter_date) enc_count
  from 
    pregnancy preg
    join enc on preg.patient_id = enc.patient_id
  where 1=1
    and year(preg.est_delivery_date) > 2019
    and year(enc.encounter_date) > 2019
    and preg.org in ('ac','ochin')
    and preg.data_lot = 'LOT 2'
  group by 1,2
)
group by 1,2
order by 1,2


-- COMMAND ----------

-- * * *
-- 
-- Diabetes and Gestational diab by org
-- 
-- * * *
 
select
  diab.org,
  diab.category,
  format_number(count(distinct diab.patient_id), 0) distinct_patient_count
from
  enc
  join diabetes diab on diab.patient_id = enc.patient_id and year(enc.encounter_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2

-- COMMAND ----------

-- * * * 
-- 
-- Diabetes Testing:
-- Distribution of different diabetes tests by test type
-- 
-- * * *
 
select
  org,
  if(preg_patient_id is null, 'No Pregnancy', 'Pregnancy') pregnancy,
  format_number(count(distinct hba1_c_test),0) hba1_c_test,
  format_number(count(distinct fglu_test),0) fglu_test,
  format_number(count(distinct ogtt_test),0) ogtt_test
from (
  select
    obs.org,
    obs.patient_id,
    preg.patient_id preg_patient_id,
    if(fglu_result is null, null, obs.patient_id) fglu_test,
    if(hba1_c_result is null, null, obs.patient_id) hba1_c_test,
    if(ogttresult is null, null, obs.patient_id) ogtt_test
  from
    enc
    join obs on enc.patient_id = obs.patient_id
    left outer join pregnancy preg on preg.patient_id = obs.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.org in ('ac', 'ochin')
    and enc.data_lot = 'LOT 2'
  )
group by 1,2

union
select
  org,
  if(preg_patient_id is null, 'No Pregnancy', 'Pregnancy') pregnancy,
  format_number(count(distinct hba1_c_test),0) hba1_c_test,
  format_number(count(distinct fglu_test),0) fglu_test,
  format_number(count(distinct ogtt_test),0) ogtt_test
from (
 select
    obs.org,
    obs.patient_id,
    preg.patient_id preg_patient_id,
    if(fglu_result is null, null, obs.patient_id) fglu_test,
    if(hb_a1c_result is null, null, obs.patient_id) hba1_c_test,
    if(ogttresult is null, null, obs.patient_id) ogtt_test
  from
    enc
    join lab obs on enc.patient_id = obs.patient_id
    left outer join pregnancy preg on preg.patient_id = obs.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.org in ('ac', 'ochin')
    and enc.data_lot = 'LOT 2'
)
group by 1,2

order by 2,1
;
