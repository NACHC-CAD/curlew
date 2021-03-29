-- Databricks notebook source
-- * * *
--
-- HCN
-- COVID BRONZE BASIC TABLES SCRIPT 2021-03-20
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- INIT THE SESSION
--
-- * * *

use covid_bronze;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
-- 
-- DEMO HCN
-- 
-- * * *

drop table if exists demo_hcn;
create table demo_hcn using delta as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(last(2020 - age) as integer) as year_of_birth,
  cast(last(sex_assigned_at_birth) as string) as sex,
  cast(last(race) as string) as race,
  cast(last(ethnic_group) as string) as ethnicity,
  cast(last(language) as string) as language,
  cast(null as string) as health_center_id
from 
  (select distinct * from covid.flat order by start_date)
where 1=1 
  and org = 'hcn'
group by 1,2,3,4,5,11
);

-- COMMAND ----------

-- * * *
--
-- ENC HCN
--
-- * * *

drop table if exists enc_hcn;
create table enc_hcn using delta as (

-- flat
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat(patient_id, '|', recorded_date_string) encounter_id,
  recorded_date as encounter_date,
  recorded_date_string as encounter_date_string,
  cast(null as string) as encounter_type
from 
  covid.flat
where 1=1 
  and org = 'hcn'

);

-- COMMAND ----------

-- * * *
-- 
-- LAB HCN
--
-- * * *

drop table if exists lab_hcn;
create table lab_hcn using delta as (
select distinct
  lab.org,
  lab.data_lot,
  lab.raw_table,
  lab.org_patient_id,
  lab.patient_id,
  start_date as test_date,
  loinc_code test_code,
  'loinc' as test_code_system,
  result_description test_name,
  cast(null as string) as test_category,
  result_value_text as test_result
from 
  covid.flat lab
where 1=1
  and org = 'hcn'
  and (result_code is not null or result_description is not null)
);

-- COMMAND ----------

-- * * *
--
-- DX HCN
--
-- * * *

drop table if exists dx_hcn;

create table dx_hcn as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  encounter_id,
  coalesce(onset_date,dx_date) as start_date,
  stop_date as stop_date,
  loinc_code code,
  'icd10' as code_system,
  loinc_description description,
  dx_category
from
  covid.dx
where
  org = 'hcn'
);



-- COMMAND ----------

-- * * *
--
-- SYMP HCN
--
-- * * *

-- NO DATA AVAILABLE AT THIS TIME

-- COMMAND ----------

-- * * *
--
-- SDOH HCN
--
-- * * *

drop table if exists sdoh_hcn;
create table sdoh_hcn as (
-- acres_disabled_flag
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_disabled_flag' as observation_name,
  cast(null as string) as survey_question,
  acres_disabled_flag observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and acres_disabled_flag is not null

union

-- acres_homeless_flag
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_homeless_flag' as observation_name,
  cast(null as string) as survey_question,
  acres_homeless_flag observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and acres_homeless_flag is not null

union

-- acres_normalized_agricultural_worker_category
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_normalized_agricultural_worker_category' as observation_name,
  cast(null as string) as survey_question,
  worker_category observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and worker_category is not null

union

-- acres_normalized_housing_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_normalized_housing_status' as observation_name,
  cast(null as string) as survey_question,
  housing_status observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and housing_status is not null

union

-- acres_normalized_veteran_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_normalized_veteran_status' as observation_name,
  cast(null as string) as survey_question,
  veteran_status observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and veteran_status is not null

union

-- acres_normalized_veteran_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'acres_normalized_veteran_status' as observation_name,
  cast(null as string) as survey_question,
  veteran_status observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and veteran_status is not null

union

-- household_size
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'household_size' as observation_name,
  cast(null as string) as survey_question,
  household_size observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and household_size is not null

union

-- poverty_level
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'poverty_level' as observation_name,
  cast(null as string) as survey_question,
  poverty_level observation_value 
from
  covid.flat
where 1=1
  and org = 'hcn'
  and poverty_level is not null
);
