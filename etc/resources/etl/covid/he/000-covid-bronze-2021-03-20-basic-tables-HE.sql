-- Databricks notebook source
-- * * *
--
-- HE
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
-- 3.) DEMO HE
-- 
-- * * *

drop table if exists demo_he;
create table demo_he using delta as (
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
  and org = 'he'
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
  org,
  data_lot,
  raw_table,
  org_patient_id,
  patient_id,
  concat('dummy|', patient_id, '|', encounter_date_string) encounter_id,
  encounter_date,
  encounter_date_string,
  'dummy' as encounter_type
from 
  covid.he_dx
where 1=1 
  and org = 'he'

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
-- SYMP HE
--
-- * * *

-- TODO: NEED TO BE EXTRACTED FROM COVID.HE_DX TABLE

-- COMMAND ----------

-- * * *
--
-- SDOH HE
--
-- * * *

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

