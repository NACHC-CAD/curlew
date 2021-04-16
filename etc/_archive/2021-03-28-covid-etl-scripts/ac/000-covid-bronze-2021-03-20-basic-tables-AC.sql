-- Databricks notebook source
-- * * *
--
-- AC (AllianceChicago)
-- COVID BRONZE BASIC TABLES SCRIPT 2021-03-20
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- INIT THE SESSION
--
-- * * *

create database if not exists covid_bronze;
use covid_bronze;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
-- 
-- DEMO AC
-- 
-- * * *

drop table if exists demo_ac;
create table demo_ac using delta as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  2020 - age as year_of_birth,
  sex,
  race,
  ethnicity,
  language,
  health_center_id
from 
  covid.demo 
where 1=1 
  and org = 'ac'
);
  

-- COMMAND ----------

-- * * *
--
-- ENC AC
--
-- * * *

drop table if exists enc_ac;
create table enc_ac using delta as (

-- enc
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  encounter_id,
  encounter_date,
  encounter_date_string,
  visit_category as encounter_type
from
  covid.enc
where 1=1
  and org = 'ac'
  
union

-- dx
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, coalesce(onset_date_string,stop_date_string)) encounter_id,
  coalesce(onset_date,stop_date) encounter_date,
  coalesce(onset_date_string,stop_date_string) encounter_date_string,
  'dummy' as encounter_type
from
  covid.dx
where 1=1
  and org = 'ac'
  
union

-- symp
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|',patient_id,observation_date_string) encounter_id,
  observation_date as encounter_date,
  observation_date_string as encounter_date_string,
  'dummy' as encounter_type
from
  covid.symp
where
  org = 'ac'
  
);

-- COMMAND ----------

-- * * *
-- 
-- LAB AC
--
-- * * *

drop table if exists lab_ac;
create table lab_ac using delta as (
select distinct 
  lab.org,
  lab.data_lot,
  lab.raw_table,
  lab.org_patient_id,
  lab.patient_id,
  coalesce(lab.observation_date, lab.ordering_date, lab.diagnosis_date) as test_date,
  lab.loinc_code test_code,
  'loinc' as test_code_system,
  lab.result_test_name test_name,
  lab.test_type as test_category,
  coalesce(lab.result, lab.observation_value) as test_result
from
  covid.lab lab
where 1=1 
  and coalesce(observation_date, ordering_date, diagnosis_date) is not null
  and lab.org = 'ac'
);

-- COMMAND ----------

-- * * *
--
-- DX AC
--
-- * * *

drop table if exists dx_ac;
create table dx_ac as (
  select distinct
    org,
    data_lot,
    raw_table,
    org_patient_id,
    patient_id,
    concat('dummy|', patient_id, coalesce(onset_date_string,stop_date_string)) encounter_id,
    onset_date as start_date,
    stop_date as stop_date,
    icd10_code code,
    'icd10' as code_system,
    description,
    dx_category
  from
    covid.dx
  where
    org = 'ac'
);

-- COMMAND ----------

-- * * *
--
-- SYMP AC
--
-- * * *

drop table if exists symp_ac;
create table symp_ac as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|',patient_id,observation_date_string) encounter_id,
  observation_date as encounter_date,
  observation_name,
  observation_description,
  observation_value
from
  covid.symp
where
  org = 'ac'
);

-- COMMAND ----------

-- * * *
--
-- SDOH AC
--
-- * * *

drop table if exists sdoh_ac;
create table sdoh_ac as (
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, '|', observation_date_string) encounter_id,
  observation_date encounter_date,
  name as observation_name,
  description as survey_question,
  observation_value 
from
  covid.sdoh
where
  org = 'ac'
);
