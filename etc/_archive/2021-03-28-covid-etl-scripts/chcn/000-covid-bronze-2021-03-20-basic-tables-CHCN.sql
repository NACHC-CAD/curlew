-- Databricks notebook source
-- * * *
--
-- CHCN
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
-- 2.) DEMO CHCN
-- 
-- * * *

drop table if exists demo_chcn;
create table demo_chcn using delta as (
select distinct 
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  year_of_birth,
  sex,
  race,
  ethnicity,
  language,
  health_center_id
from 
  covid.demo
where 1=1 
  and org = 'chcn'
);

-- COMMAND ----------

-- * * *
--
-- 2.) ENC CHCN
--
-- * * *

drop table if exists enc_chcn;
create table enc_chcn using delta as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  encounter_id,
  encounter_date,
  encounter_date_string,
  pos_type as encounter_type
from
  covid.enc
where 1=1
  and org = 'chcn'
);

-- COMMAND ----------

-- * * *
-- 
-- 2.) LAB CHCN
--
-- * * *

drop table if exists lab_chcn;
create table lab_chcn using delta as (
select distinct 
  lab.org,
  lab.data_lot,
  lab.raw_table,
  lab.org_patient_id,
  lab.patient_id,
  coalesce(lab.observation_date, lab.ordering_date) as test_date,
  loinc_code test_code,
  'loinc' as test_code_system,
  lab.procedure_name test_name,
  lab.test_type as test_category,
  coalesce(lab.result, lab.observation_value) as test_result
from
  covid.lab lab
where 1=1 
  and coalesce(observation_date, ordering_date) is not null
  and lab.org = 'chcn'
);

-- COMMAND ----------

-- * * *
--
-- 2.) DX CHCN
--
-- * * *

drop table if exists dx_chcn;

create table dx_chcn as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  encounter_id,
  coalesce(onset_date,dx_date) as start_date,
  stop_date as stop_date,
  icd_code code,
  'icd10' as code_system,
  description,
  dx_category
from
  covid.dx
where
  org = 'chcn'
);


-- COMMAND ----------

-- * * *
--
-- SYMP CHCN
--
-- * * *

create table symp_chcn as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  encounter_id,
  contact_date as encounter_date,
  symptom as observation_name,
  cast(null as string) observation_description,
  1 as observation_value
from
  covid.symp
where
  org = 'chcn'
);

-- COMMAND ----------

-- * * *
--
-- SDOH CHCN
--
-- * * *

drop table if exists sdoh_chcn;
create table sdoh_chcn as (

-- child care security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'child_care_security' as observation_name,
  cast(null as string) as survey_question,
  child_care_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and child_care_security is not null

union

-- clothing security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'clothing_security' as observation_name,
  cast(null as string) as survey_question,
  clothing_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and clothing_security is not null

union

-- education
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'education' as observation_name,
  cast(null as string) as survey_question,
  education observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and education is not null

union

-- employment
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'employment' as observation_name,
  cast(null as string) as survey_question,
  employment observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and employment is not null

union

-- family_size
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'family_size' as observation_name,
  cast(null as string) as survey_question,
  family_size observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and family_size is not null

union

-- food_security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'food_security' as observation_name,
  cast(null as string) as survey_question,
  food_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and food_security is not null

union

-- health_care_security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'health_care_security' as observation_name,
  cast(null as string) as survey_question,
  health_care_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and health_care_security is not null

union

-- homeless_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'homeless_status' as observation_name,
  cast(null as string) as survey_question,
  homeless_status observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and homeless_status is not null

union

-- housing_security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'housing_security' as observation_name,
  cast(null as string) as survey_question,
  housing_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and housing_security is not null

union

-- migrant_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'migrant_status' as observation_name,
  cast(null as string) as survey_question,
  migrant_status observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and migrant_status is not null

union

-- other_security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'other_security' as observation_name,
  cast(null as string) as survey_question,
  other_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and other_security is not null

union

-- phone_security
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'phone_security' as observation_name,
  cast(null as string) as survey_question,
  phone_security observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and phone_security is not null

union

-- social_integration
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'social_integration' as observation_name,
  cast(null as string) as survey_question,
  social_integration observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and social_integration is not null
  
union

-- stress
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'stress' as observation_name,
  cast(null as string) as survey_question,
  stress observation_value 
from
  covid.demo
where 1=1
  and org = 'chcn'
  and stress is not null
);
