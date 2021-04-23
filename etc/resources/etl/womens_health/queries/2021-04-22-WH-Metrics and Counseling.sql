-- Databricks notebook source
-- * * *
--
-- WOMENS_HEALTH BASIC METRICS 
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
-- TOTAL PATIENTS, COUNSELING, INCLUDE PATIENTS, AND INCLUDE PATIENTS WITH COUNSELING (TABLE)
--
-- * * *

select '1.) Total Patients' metric, format_number(count(distinct patient_id),0) count 
from enc 
where data_lot = 'LOT 2' and year(enc.encounter_date) > 2019

union 

select '2.) Counseled Patients' metric, format_number(count(distinct patient_id),0) count 
from counseling
where data_lot = 'LOT 2' and year(encounter_date) > 2019

union

select '3.) Included Patients' metric, format_number(count(distinct enc.patient_id),0) count 
from enc join include_for_contraception inc on inc.patient_id = enc.patient_id
where enc.data_lot = 'LOT 2' and year(enc.encounter_date) > 2019

union

select '4.) Included Patients with Counseling' metric, format_number(count(distinct enc.patient_id),0) count 
from enc join include_for_contraception_with_counseling inc on inc.patient_id = enc.patient_id
where enc.data_lot = 'LOT 2' and year(enc.encounter_date) > 2019

order by 1
;

-- COMMAND ----------

-- * * *
--
-- DISTRIBUTION OF CONTRACEPTION FOR TOTAL POPULATION (CHART)
--
-- * * *

select
  cat.category `Category`,
  if(couns.patient_id is null, 'No', 'Yes') `Counseling`,
  count(distinct inc.patient_id) `Distinct Patients`
from
  include_for_contraception inc
  join enc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = inc.patient_id
  left outer join counseling couns on couns.patient_id = inc.patient_id
where 1=1
  and cat.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and cat.category != 'NOT BIRTH CONTROL'
group by 1,2
order by 3 desc, 1, 2

-- COMMAND ----------

-- * * *
--
-- DISTRIBUTION OF CONTRACEPTION FOR TOTAL POPULATION (TABLE)
--
-- * * *

select
  cat.category `Category`,
  if(couns.patient_id is null, 'No', 'Yes') `Counseling`,
  format_number(count(distinct inc.patient_id),0) `Distinct Patients`
from
  include_for_contraception inc
  join enc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = inc.patient_id
  left outer join counseling couns on couns.patient_id = inc.patient_id
where 1=1
  and cat.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and cat.category != 'NOT BIRTH CONTROL'
group by 1,2
order by 3 desc, 1, 2

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION DISTRIBUTION: COUNSELING V. NO COUNSELING (CHART)
--
-- * * *

select
  category,
  not_counseled,
  counseled,
  total_included_patients,
  total_included_patients_with_counseling,
  (total_included_patients - total_included_patients_with_counseling) total_included_patients_with_out_counseling,
  not_counseled/(total_included_patients - total_included_patients_with_counseling) * 100 pct_not_counseled,
  counseled/total_included_patients_with_counseling * 100 pct_counseled
from
  contraception_category_counseling
  -- data are already filtered for lot 2 and enc date in contraception_category_counseling
order by 3 desc
;

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION DISTRIBUTION: COUNSELING V. NO COUNSELING (TABLE)
--
-- * * *

select
  category,
  not_counseled,
  counseled,
  format_number(total_included_patients,0) total_included_patients,
  format_number(total_included_patients_with_counseling,0) total_included_patients_with_counseling,
  format_number((total_included_patients - total_included_patients_with_counseling),0) total_included_patients_with_out_counseling,
  format_number(not_counseled/(total_included_patients - total_included_patients_with_counseling) * 100, 4) pct_counseled,
  format_number(counseled/total_included_patients_with_counseling * 100,4) pct_not_counseled
from
  contraception_category_counseling
  -- data are already filtered for lot 2 and enc date in contraception_category_counseling
order by 3 desc
;
