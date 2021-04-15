-- Databricks notebook source
-- * * *
--
-- COVID BRONZE BASIC TABLES SCRIPT 2021-04-15
-- This script creates everything up to the "_SRC" tables.  
-- The "_SRC" tables are subsequently joint to the "_NACHC" tables to create the base tables (e.g. demo, enc, dx, etc.).
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

drop table if exists demo_ac;
drop table if exists demo_chcn;
drop table if exists demo_he;
drop table if exists demo_hcn;

drop table if exists enc_ac;
drop table if exists enc_chcn;
drop table if exists enc_he;
drop table if exists enc_hcn;

drop table if exists dx_ac;
drop table if exists dx_chcn;
drop table if exists dx_he;
drop table if exists dx_hcn;

drop table if exists lab_ac;
drop table if exists lab_chcn;
drop table if exists lab_he;
drop table if exists lab_hcn;

drop table if exists sdoh_ac;
drop table if exists sdoh_chcn;
drop table if exists sdoh_he;
drop table if exists sdoh_hcn;

drop table if exists symp_ac;
drop table if exists symp_chcn;
drop table if exists symp_he;
drop table if exists symp_hcn;

drop table if exists demo_src;
drop table if exists enc_src;
drop table if exists lab_src;
drop table if exists dx_src;
drop table if exists sdoh_src;
drop table if exists symp_src;

drop table if exists demo;
drop table if exists enc;
drop table if exists lab;
drop table if exists dx;
drop table if exists sdoh;
drop table if exists symp;

drop table if exists demo_race_nachc;
drop table if exists demo_sex_nachc;
drop table if exists lab_test_result_nachc;
drop table if exists lab_test_category_nachc;

drop table if exists patient_covid_exp;
drop table if exists patient_race;

drop table if exists cur;

show tables;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show databases;

-- COMMAND ----------

show tables in covid;

-- COMMAND ----------

-- * * *
--
-- DEMO TABLES
-- 
-- * * *

-- COMMAND ----------

-- * * *
-- 
-- 1.) DEMO AC
-- 
-- * * *

drop table if exists demo_ac;
create table demo_ac using delta as (
select distinct
  org_patient_id,
  patient_id,
  2020 - last(age) as year_of_birth,
  last(sex) sex,
  last(race) race,
  last(ethnicity) ethnicity,
  last(language) language,
  last(state) state,
  last(zip) zip,
  last(health_center_id) health_center_id,
  last(org) org,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(data_lot) data_lot,
  last(raw_table) raw_table
from 
  (select * from covid.demo order by provided_date)
where 1=1 
  and org = 'ac'
  and patient_id is not null
group by 1,2
);
  

-- COMMAND ----------

-- * * *
-- 
-- 2.) DEMO CHCN
-- 
-- * * *

drop table if exists demo_chcn;
create table demo_chcn using delta as (
select distinct 
  org_patient_id,
  patient_id,
  last(year_of_birth) year_of_birth,
  last(sex) sex,
  last(race) race,
  last(ethnicity) ethnicity,
  last(language) language,
  last(state) state,
  last(zip) zip,
  last(health_center_id) health_center_id,
  last(org) org,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(data_lot) data_lot,
  last(raw_table) raw_table
from 
  covid.demo
where 1=1 
  and org = 'chcn'
  and patient_id is not null
group by 
  org_patient_id,
  patient_id
);

-- COMMAND ----------

-- * * *
-- 
-- 3.) DEMO HE
-- 
-- * * *

drop table if exists demo_he;
create table demo_he using delta as (
select distinct
  org_patient_id,
  patient_id,
  2020 - last(age) as year_of_birth,
  last(sex) sex,
  last(race) race,
  last(ethnicity) ethnicity,
  last(language) language,
  last(state) state,
  last(zip) zip,
  last(health_center_id) health_center_id,
  last(org) org,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(data_lot) data_lot,
  last(raw_table) raw_table
from 
  (select * from covid.demo order by raw_table)
where 1=1 
  and org = 'he'
  and patient_id is not null
group by 
  patient_id,
  org_patient_id
);

-- COMMAND ----------

-- * * *
-- 
-- 4.) DEMO HCN
-- 
-- * * *

drop table if exists demo_hcn;
create table demo_hcn using delta as (
select distinct
  org_patient_id,
  patient_id,
  cast(last(2020 - age) as integer) as year_of_birth,
  cast(last(sex_assigned_at_birth) as string) as sex,
  cast(last(race) as string) as race,
  cast(last(ethnic_group) as string) as ethnicity,
  cast(last(language) as string) as language,
  last(patient_state_cd) as state,
  cast(lpad(last(patient_zip_cd), 5, '0') as string) as zip,
  cast(null as string) as health_center_id,
  last(org) org,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(data_lot) data_lot,
  last(raw_table) raw_table
from 
  (select distinct * from covid.flat order by start_date)
where 1=1 
  and org = 'hcn'
  and patient_id is not null
group by 
  patient_id,
  org_patient_id
);

-- COMMAND ----------

-- * * *
--
-- ENC
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- 1.) ENC AC
--
-- * * *

drop table if exists enc_ac;
create table enc_ac using delta as 
select distinct
  last(org) org,
  last(data_lot) data_lot,
  last(raw_table) raw_table,
  last(org_patient_id) org_patient_id,
  last(patient_id) patient_id,
  encounter_id,
  last(encounter_date) encounter_date,
  last(encounter_date_string) encounter_date_string,
  last(encounter_type) as encounter_type,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(is_dummy) is_dummy

from (

  -- enc
  select distinct
    last(org) org,
    last(data_lot) data_lot,
    last(raw_table) raw_table,
    last(org_patient_id) org_patient_id,
    last(patient_id) patient_id,
    encounter_id,
    last(encounter_date) encounter_date,
    last(encounter_date_string) encounter_date_string,
    last(visit_category) as encounter_type,
    last(provided_by) provided_by,
    last(provided_date) provided_date,
    0 as is_dummy
  from
    (select * from covid.enc order by provided_date)
  where 1=1
    and org = 'ac'
    and encounter_id is not null
  group by encounter_id

  union

  -- dx
  select distinct
    last(org) org,
    last(data_lot) data_lot,
    last(raw_table) raw_table,
    last(org_patient_id) org_patient_id,
    last(patient_id) patient_id,
    concat('dummy|', patient_id, coalesce(onset_date_string,stop_date_string)) encounter_id,
    last(coalesce(onset_date,stop_date)) encounter_date,
    last(coalesce(onset_date_string,stop_date_string)) encounter_date_string,
    'dummy' as encounter_type,
    last(provided_by) provided_by,
    last(provided_date) provided_date,
    1 as is_dummy
  from
    (select * from covid.dx order by provided_date)
  where 1=1
    and org = 'ac'
    and patient_id is not null
    and coalesce(onset_date_string,stop_date_string) is not null
  group by 6  

  union

  -- symp
  select distinct
    last(org) org,
    last(data_lot) data_lot,
    last(raw_table) raw_table,
    last(org_patient_id) org_patient_id,
    last(patient_id) patient_id,
    concat('dummy|',patient_id,observation_date_string) encounter_id,
    last(observation_date) as encounter_date,
    last(observation_date_string) as encounter_date_string,
    'dummy' as encounter_type,
    last(provided_by) provided_by,
    last(provided_date) provided_date,
    1 as is_dummy
  from
    (select * from covid.symp order by provided_date)
  where
    org = 'ac'
    and patient_id is not null and observation_date_string is not null
  group by 6

)
group by 6
order by provided_date, is_dummy;

-- COMMAND ----------

-- * * *
--
-- 2.) ENC CHCN
--
-- * * *

drop table if exists enc_chcn;
create table enc_chcn using delta as (
select distinct
  last(org) org,
  last(data_lot) data_lot,
  last(raw_table) raw_table,
  last(org_patient_id) org_patient_id,
  last(patient_id) patient_id,
  encounter_id,
  last(encounter_date) encounter_date,
  last(encounter_date_string) encounter_date_string,
  last(pos_type) as encounter_type,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  0 as is_dummy
from
  (select * from covid.enc order by provided_date)
where 1=1
  and org = 'chcn'
  and encounter_id is not null
group by 6
);

-- COMMAND ----------

-- * * *
--
-- 3) ENC HE
--
-- * * *

drop table if exists enc_he;
create table enc_he using delta as (

-- he_dx
select distinct
  last(org) org,
  last(data_lot) data_lot,
  last(raw_table) raw_table,
  last(org_patient_id) org_patient_id,
  last(patient_id) patient_id,
  concat('dummy|', patient_id, '|', encounter_date_string) encounter_id,
  last(encounter_date) encounter_date,
  last(encounter_date_string) encounter_data_string,
  'dummy' as encounter_type,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  0 as is_dummy
from 
  covid.he_dx
where 1=1 
  and org = 'he'
  and patient_id is not null
  and encounter_date_string is not null
group by 6
);

-- COMMAND ----------

-- * * *
--
-- 4) ENC HCN
--
-- * * *

drop table if exists enc_hcn;
create table enc_hcn using delta as (

select distinct
  last(org) org,
  last(data_lot) data_lot,
  last(raw_table) raw_table,
  last(org_patient_id) org_patient_id,
  last(patient_id) patient_id,
  encounter_id,
  last(encounter_date) encounter_date,
  last(encounter_date_string) encounter_date_string,
  last(encounter_type) as encounter_type,
  last(provided_by) provided_by,
  last(provided_date) provided_date,
  last(is_dummy) is_dummy

from (

  -- dx
  select distinct
    last(org) org,
    last(data_lot) data_lot,
    last(raw_table) raw_table,
    last(org_patient_id) org_patient_id,
    last(patient_id) patient_id,
    concat(patient_id, '|', recorded_date_string) encounter_id,
    last(recorded_date) as encounter_date,
    last(recorded_date_string) as encounter_date_string,
    cast(null as string) as encounter_type,
    last(provided_by) provided_by,
    last(provided_date) provided_date,
    0 as is_dummy
  from 
    (select * from covid.dx order by provided_date) dx
  where 1=1 
    and org = 'hcn'
    and patient_id is not null
    and recorded_date_string is not null
  group by 6

  union

  -- flat
  select distinct
    last(org) org,
    last(data_lot) data_lot,
    last(raw_table) raw_table,
    last(org_patient_id) org_patient_id,
    last(patient_id) patient_id,
    concat(patient_id, '|', recorded_date_string) encounter_id,
    last(recorded_date) as encounter_date,
    last(recorded_date_string) as encounter_date_string,
    cast(null as string) as encounter_type,
    last(provided_by) provided_by,
    last(provided_date) provided_date,
    0 as is_dummy
  from 
    (select * from covid.flat order by provided_date) flat
  where 1=1 
    and org = 'hcn'
    and patient_id is not null
    and recorded_date_string is not null
  group by 6
  order by provided_date, is_dummy
  ) enc
group by 6
);

-- COMMAND ----------

-- * * *
-- 
-- LAB
--
-- * * *

-- COMMAND ----------

-- * * *
-- 
-- 1.) LAB AC
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
  coalesce(lab.observation_date, lab.ordering_date) as test_date,
  lab.loinc_code test_code,
  'loinc' as test_code_system,
  lab.result_test_name test_name,
  lab.test_type as test_category,
  coalesce(lab.result, lab.observation_value) as test_result
from
  covid.lab lab
where 1=1 
  and coalesce(observation_date, ordering_date) is not null
  and lab.org = 'ac'
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
-- 3.) LAB HE
--
-- * * *

drop table if exists lab_he;
create table lab_he using delta as (
-- ANTIGEN
select distinct 
  lab.org,
  lab.data_lot,
  lab.raw_table,
  lab.org_patient_id,
  lab.patient_id,
  encounter_date as test_date,
  antigen_loinccode test_code,
  'loinc' as test_code_system,
  cast(null as string) test_name,
  'COVID_ANTIGEN_TEST' as test_category,
  antigen_test_result as test_result
from
  covid.lab lab
where 1=1 
  and antigen_test_result is not null
  and encounter_date is not null
  and lab.org = 'he'
  
union all

-- ANTIBODY
select distinct 
  lab.org,
  lab.data_lot,
  lab.raw_table,
  lab.org_patient_id,
  lab.patient_id,
  encounter_date as test_date,
  antibody_loinccode test_code,
  'loinc' as test_code_system,
  cast(null as string) test_name,
  'COVID_ANTIBODY_TEST' as test_category,
  antibody_test_result as test_result
from
  covid.lab lab
where 1=1 
  and antibody_test_result is not null
  and encounter_date is not null
  and lab.org = 'he'
);

-- COMMAND ----------

-- * * *
-- 
-- 4. LAB HCN
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
  result_code test_code,
  cast(null as string) as test_code_system,
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
-- DX Stuff
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- 1.) DX AC
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
-- 3.) DX HE
--
-- * * *

drop table if exists dx_he;

create table dx_he using delta as (

-- covid exp
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, '|', encounter_date_string) encounter_id,
  encounter_date as start_date,
  cast(null as date) as stop_date,
  cast(null as string) code,
  cast(null as string) as code_system,
  cast(null as string) as description,
  'COVID_EXP' as dx_category
from
  covid.he_dx
where 1=1
  and org = 'he'
  and known_covid19_exposure = 1

union all

-- covid sus
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, '|', encounter_date_string) encounter_id,
  encounter_date as start_date,
  cast(null as date) as stop_date,
  cast(null as string) code,
  cast(null as string) as code_system,
  cast(null as string) as description,
  'COVID_SUS' as dx_category
from
  covid.he_dx
where 1=1
  and org = 'he'
  and covid19_suspected_infection = 1

union all

-- covid diag
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, '|', encounter_date_string) encounter_id,
  encounter_date as start_date,
  cast(null as date) as stop_date,
  cast(null as string) code,
  cast(null as string) as code_system,
  cast(null as string) as description,
  'COVID_DIAG' as dx_category
from
  covid.he_dx
where 1=1
  and org = 'he'
  and covid19_confirmed_diagnosis = 1

);

-- COMMAND ----------

-- * * *
--
-- 4.) DX HCN
--
-- * * *

drop table if exists dx_hcn;

create table dx_hcn as (

-- dx
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat(patient_id, '|', recorded_date_string) encounter_id,
  coalesce(onset_date,dx_date) as start_date,
  stop_date as stop_date,
  code code,
  'icd10' as code_system,
  code_description description,
  dx_category
from
  covid.dx
where
  org = 'hcn'

union

-- flat
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat(patient_id, '|', recorded_date_string) encounter_id,
  start_date as start_date,
  null as stop_date,
  code code,
  'icd10' as code_system,
  code_description description,
  null as dx_category
from
  covid.flat
where
  org = 'hcn'
  and (code is not null or code_description is not null)
);



-- COMMAND ----------

-- * * *
--
-- SYMP
--
-- * * *

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

select * from covid.symp where org = 'chcn'

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
  encounter_date,
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
-- SYMP HE
--
-- * * *

-- NO DATA AVAILABLE AT THIS TIME

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
-- SDOH 
--
-- * * *

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

-- COMMAND ----------

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

-- COMMAND ----------

drop table if exists sdoh_he;
create table sdoh_he as (

-- annual_income
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'annual_income' as observation_name,
  cast(null as string) as survey_question,
  annual_income observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and annual_income is not null

union

-- connectedness14
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'connectedness14' as observation_name,
  cast(null as string) as survey_question,
  connectedness14 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and connectedness14 is not null

union

-- education10
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'education10' as observation_name,
  cast(null as string) as survey_question,
  education10 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and education10 is not null

union

-- empl_status
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'empl_status' as observation_name,
  cast(null as string) as survey_question,
  empl_status observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and empl_status is not null

union

-- family_size6
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'family_size6' as observation_name,
  cast(null as string) as survey_question,
  family_size6 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and family_size6 is not null

union

-- farm_worker
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'farm_worker' as observation_name,
  cast(null as string) as survey_question,
  farm_worker observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and farm_worker is not null

union

-- fearof_partner21
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'fearof_partner21' as observation_name,
  cast(null as string) as survey_question,
  fearof_partner21 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and fearof_partner21 is not null

union

-- feel_safe20
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'feel_safe20' as observation_name,
  cast(null as string) as survey_question,
  feel_safe20 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and feel_safe20 is not null

union

-- homeless
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'homeless' as observation_name,
  cast(null as string) as survey_question,
  homeless observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and homeless is not null

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
  and org = 'he'
  and homeless_status is not null

union

-- housing_situation7
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'housing_situation7' as observation_name,
  cast(null as string) as survey_question,
  housing_situation7 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and housing_situation7 is not null

union

-- incarceration16
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'incarceration16' as observation_name,
  cast(null as string) as survey_question,
  incarceration16 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and incarceration16 is not null

union

-- last_of_poverty_level
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'last_of_poverty_level' as observation_name,
  cast(null as string) as survey_question,
  last_of_poverty_level observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and last_of_poverty_level is not null

union

-- migrant3
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'migrant3' as observation_name,
  cast(null as string) as survey_question,
  migrant3 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and migrant3 is not null

union

-- needs_childcare13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_childcare13' as observation_name,
  cast(null as string) as survey_question,
  needs_childcare13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_childcare13 is not null

union

-- needs_clothing13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_clothing13' as observation_name,
  cast(null as string) as survey_question,
  needs_clothing13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_clothing13 is not null

union

-- needs_food13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_food13' as observation_name,
  cast(null as string) as survey_question,
  needs_food13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_food13 is not null

union

-- needs_medicine_medical13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_medicine_medical13' as observation_name,
  cast(null as string) as survey_question,
  needs_medicine_medical13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_medicine_medical13 is not null

union

-- needs_other13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_other13' as observation_name,
  cast(null as string) as survey_question,
  needs_other13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_other13 is not null

union

-- needs_phone13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_phone13' as observation_name,
  cast(null as string) as survey_question,
  needs_phone13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_phone13 is not null

union

-- needs_refused_answer13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_refused_answer13' as observation_name,
  cast(null as string) as survey_question,
  needs_refused_answer13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_refused_answer13 is not null

union

-- needs_utilities13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs_utilities13' as observation_name,
  cast(null as string) as survey_question,
  needs_utilities13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs_utilities13 is not null

union

-- needs13
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'needs13' as observation_name,
  cast(null as string) as survey_question,
  needs13 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and needs13 is not null

union

-- poverty_group
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'poverty_group' as observation_name,
  cast(null as string) as survey_question,
  poverty_group observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and poverty_group is not null

union

-- public_housing
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'public_housing' as observation_name,
  cast(null as string) as survey_question,
  public_housing observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and public_housing is not null

union

-- refugee18
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'refugee18' as observation_name,
  cast(null as string) as survey_question,
  refugee18 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and refugee18 is not null

union

-- seasonal3
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'seasonal3' as observation_name,
  cast(null as string) as survey_question,
  seasonal3 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and seasonal3 is not null

union

-- stress15
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'stress15' as observation_name,
  cast(null as string) as survey_question,
  stress15 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and stress15 is not null

union

-- transportation17
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'transportation17' as observation_name,
  cast(null as string) as survey_question,
  transportation17 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and transportation17 is not null

union

-- udshomeless_status8a
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'udshomeless_status8a' as observation_name,
  cast(null as string) as survey_question,
  udshomeless_status8a observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and udshomeless_status8a is not null

union

-- udshomeless_status8a
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'udshomeless_status8a' as observation_name,
  cast(null as string) as survey_question,
  udshomeless_status8a observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and udshomeless_status8a is not null

union

-- udshomeless8a
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'udshomeless8a' as observation_name,
  cast(null as string) as survey_question,
  udshomeless8a observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and udshomeless8a is not null

union

-- worried_about_housing8
select distinct
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  cast(null as string) encounter_id,
  cast(null as date) encounter_date,
  'worried_about_housing8' as observation_name,
  cast(null as string) as survey_question,
  worried_about_housing8 observation_value 
from
  covid.demo
where 1=1
  and org = 'he'
  and worried_about_housing8 is not null
);


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

-- COMMAND ----------

-- * * *
--
-- CREATE BASE TABLES
--
-- * * *

-- COMMAND ----------

drop table if exists demo_src;
create table demo_src using delta as (
  select distinct * from demo_ac
  union 
  select distinct * from demo_chcn
  union 
  select distinct * from demo_he
  union 
  select distinct * from demo_hcn
);

-- COMMAND ----------

drop table if exists enc_src;
create table enc_src using delta as (
  select distinct * from enc_ac
  union 
  select distinct * from enc_chcn
  union 
  select distinct * from enc_he
  union 
  select distinct * from enc_hcn
);

-- COMMAND ----------

drop table if exists dx_src;
create table dx_src using delta as (
  select distinct * from dx_ac
  union 
  select distinct * from dx_chcn
  union 
  select distinct * from dx_he
  union 
  select distinct * from dx_hcn
);

-- COMMAND ----------

drop table if exists lab_src;
create table lab_src using delta as (
  select distinct * from lab_ac
  union 
  select distinct * from lab_chcn
  union 
  select distinct * from lab_he
  union 
  select distinct * from lab_hcn
);

-- COMMAND ----------

drop table if exists symp_src;
create table symp_src as (
  select distinct * from symp_ac
  union
  select distinct * from symp_chcn
  -- no data available for he at this time
  -- no data available for hcn at this time
);


-- COMMAND ----------

drop table if exists sdoh_src;
create table sdoh_src as (
select distinct * from sdoh_ac
union
select distinct * from sdoh_chcn
union
select distinct * from sdoh_he
union
select distinct * from sdoh_hcn
);

