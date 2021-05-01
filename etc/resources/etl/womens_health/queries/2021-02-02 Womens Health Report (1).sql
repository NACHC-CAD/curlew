-- Databricks notebook source
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
-- TOTAL PATIENTS
-- All patients with an encounter record after 2019
-- Only 'LOT 2' data included
-- Only ac and ochin are considered
-- 
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0)
from
  enc
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- TOTAL PREGNANCIES
-- For pregnancies with estimated delivery date after 2019
-- For patients with at least one encounter after 2019
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join pregnancy preg on preg.patient_id = enc.patient_id
where 1=1
  and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- CONFIRMED PREGNANCIES
-- For pregnancies with observed delivery date after 2019
-- For patients with at least one encounter after 2019
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join conf_pregnancy conf on conf.pat_id = enc.org_patient_id
where 1=1
  and year(conf.delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION INCLUDE GROUP
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join include_for_contraception inc on inc.patient_id = enc.patient_id
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION BY CATEGORY
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  cat.category,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join include_for_contraception inc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = enc.patient_id
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1,2
order by 2,1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION BY SUB-CATEGORY
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  cat.category,
  cat.sub_category,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join include_for_contraception inc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = enc.patient_id
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1,2,3
order by 2,3,1
;

-- COMMAND ----------

-- * * *
-- 
-- GESTATIONAL DIABETES
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
-- Only gestational diabetes is included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join pregnancy preg on preg.patient_id = enc.patient_id
  join diabetes diab on diab.patient_id = enc.patient_id
where 1=1
  and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and lower(diab.category) = 'gestational diabetes'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- GESTATIONAL DIABETES AND DIABETES
-- This is patients that have a diag of diab and also have a diag of GDM
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
-- Only gestational diabetes is included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) `GDM + Diabetes`
from
  enc
  join pregnancy preg on preg.patient_id = enc.patient_id
  join diabetes diab on diab.patient_id = enc.patient_id
  join diabetes gdm on gdm.patient_id = enc.patient_id
where 1=1
  and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and lower(diab.category) = 'diabetes'
  and lower(gdm.category) = 'gestational diabetes'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- ALL DIABETES
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
-- Only gestational diabetes is included
--
-- * * *

select
  enc.org,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join pregnancy preg on preg.patient_id = enc.patient_id
  join diabetes diab on diab.patient_id = enc.patient_id
where 1=1
  and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- GESTATIONAL DIABETES SCREENING
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
-- Only gestational diabetes is included
--
-- * * *

select
  enc.org,
  format_number(count(distinct a1c.patient_id),0) a1c,
  format_number(count(distinct fglu.patient_id),0) fglu, 
  format_number(count(distinct ogtt.patient_id),0) ogtt 
from
  enc
  join pregnancy preg on preg.patient_id = enc.patient_id
  join diabetes diab on diab.patient_id = enc.patient_id
  left outer join diabetes_screening a1c on a1c.patient_id = enc.patient_id and lower(a1c.test_type) = 'hba1c'
  left outer join diabetes_screening fglu on fglu.patient_id = enc.patient_id and lower(fglu.test_type) = 'fglu'
  left outer join diabetes_screening ogtt on ogtt.patient_id = enc.patient_id and lower(ogtt.test_type) = 'ogtt'
where 1=1
  and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
  and lower(diab.category) = 'gestational diabetes'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- TOTAL PREGNANCIES
-- For pregnancies with estimated delivery date after 2019
-- For patients with at least one encounter after 2019
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  org,
  count(distinct 02_weeks) as `02 Weeks`,
  count(distinct 08_weeks) as `08 Weeks`,
  count(distinct 17_weeks) as `17 Weeks`
from (
  select
    enc.org,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*2)), preg.patient_id, null) as 02_weeks,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*8)), preg.patient_id, null) as 08_weeks,
    if(enc.encounter_date < date_add(preg.est_delivery_date, (7*17)), preg.patient_id, null) as 17_weeks
  from
    enc
    join pregnancy preg on preg.patient_id = enc.patient_id
  where 1=1
    and enc.encounter_date > preg.est_delivery_date
    and year(preg.est_delivery_date) > 2019
    and year(enc.encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
    and enc.org in ('ac', 'ochin')
)
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION BY CATEGORY
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  cat.category,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join pregnancy inc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = enc.patient_id
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1,2
order by 2,1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION BY CATEGORY
-- For patients with at least one encounter after 2019
-- For patients included in the INCLUDE_FOR_CONTRACEPTION group
-- Only 'LOT 2' is included
-- Only ac and ochin are included
--
-- * * *

select
  enc.org,
  cat.category,
  cat.sub_category,
  format_number(count(distinct enc.patient_id),0) 
from
  enc
  join pregnancy inc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = enc.patient_id
where 1=1
  and enc.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and enc.org in ('ac', 'ochin')
group by 1,2,3
order by 2,3,1
;
