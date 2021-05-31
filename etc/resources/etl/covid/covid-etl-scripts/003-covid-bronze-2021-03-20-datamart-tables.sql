-- Databricks notebook source
-- * * *
--
-- COVID BRONZE MAPPING TABLES SCRIPT 2021-04-17
-- THIS SCRIPT CREATES CUSTOM TABLES FOR SPECIFIC BUSINESS REQUIREMENTS
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
select 'INSTANCE SET TO COVID_BRONZE';

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table if exists patient_race;
create table patient_race as (
  select
    org,
    patient_id,
    (case
      when lower(demo.ethnicity) in ('hispanic', 'hispanic or latino') then 'Latino'
      when lower(demo.race_nachc) in ('latino') then 'Latino'
      when lower(demo.race_nachc) in ('ai/an', 'alaskan native', 'american indian', 'american indian or alaska native') then 'AI/AN'
      when lower(demo.race_nachc) in ('asian') then 'API'
      when lower(demo.race_nachc) in ('black or african american', 'black/african american') then 'Black'
      when lower(demo.race_nachc) in ('hispanic/latino') then 'Latino'
      when lower(demo.race_nachc) in ('more than one race') then 'Other'
      when lower(demo.race_nachc) in ('native hawaiian', 'native hawaiian or other pacific islander') then 'Native Hawaiian'
      when lower(demo.race_nachc) in ('pacific islander') then 'API'
      when lower(demo.race_nachc) in ('white') then 'White'
      when lower(demo.race_nachc) in ('other', 'not collected or unknown', 'patient refused', 'patient declined', 'state prohibited', 'unknown', 'unspecified') then 'Other'
      when lower(demo.race_nachc) is null then null
      else concat('NOT MAPPED: ', demo.race)
     end
    ) race
  from
    demo
    where demo.patient_id is not null
);


-- COMMAND ----------

-- * * *
--
-- PATIENT_COVID_OBS
-- THIS IS A DATE LEVEL REPRESENTATION OF DIAG/EXP/SUS
-- 
-- * * *

drop table if exists patient_covid_obs;

create table patient_covid_obs as (

-- ac
select distinct
  org,
  start_date obs_date,  
  (case
    when code = 'U07.1' then 'DIAG'
    when code = 'Z03.818' then 'EXP'
    when code = 'Z20.828' then 'EXP'
    when code = 'Z11.59' then 'SUS'
    end
  ) category,
  patient_id,
  org_patient_id
from
  dx
where 1=1
  and UPPER(code) in ('U07.1','Z03.818','Z20.828','Z11.59')
  and org = 'ac'
    
union all

-- chcn
select distinct
  org,
  start_date obs_date,  
  (case
    when dx_category = 'COVID 19 Confirmed Diagnosis' then 'DIAG'
    when dx_category = 'COVID19 Suspected Infection' then 'SUS'
    when dx_category = 'COVID Exposure' then 'EXP'
    end
  ) category,
  patient_id,
  org_patient_id
from
  dx
where 1=1
  and lower(dx_category) like '%covid%'
  and org = 'chcn'
union all

-- he
select distinct
  org,
  start_date obs_date,  
  (case
    when dx_category = 'COVID_DIAG' then 'DIAG'
    when dx_category = 'COVID_SUS' then 'SUS'
    when dx_category = 'COVID_EXP' then 'EXP'
    end
  ) category,
  patient_id,
  org_patient_id
from
  dx
where 1=1
  and lower(dx_category) like '%covid%'
  and org = 'he'

union all

-- hcn
select distinct
  org,
  start_date obs_date,  
  (case
    when upper(code) = 'U07.1' then 'DIAG'
    when upper(code) = 'Z03.818' then 'EXP'
    when upper(code) = 'Z20.828' then 'EXP'
    when upper(code) = 'Z11.59' then 'SUS'
    end
  ) category,
  patient_id,
  org_patient_id
from
  dx
where 1=1
  and org = 'hcn'
  and code is not null
  and UPPER(code) in ('U07.1','Z03.818','Z20.828','Z11.59')
);

-- COMMAND ----------

-- * * *
--
-- PATIENT_COVID_EXP
-- (TODO: Refactor this to use the above table)
-- THIS IS A MONTH LEVEL REPRESENTATION OF DIAG/EXP/SUS
-- 
-- * * *

drop table if exists patient_covid_exp;

create table patient_covid_exp as (

-- ac
select distinct
  org,
  concat(year(start_date), '-', format_number(month(start_date),'00')) month,  
  (case
    when code = 'U07.1' then 'DIAG'
    when code = 'Z03.818' then 'EXP'
    when code = 'Z20.828' then 'EXP'
    when code = 'Z11.59' then 'SUS'
    end
  ) category,
  patient_id
from
  dx
where 1=1
  and UPPER(code) in ('U07.1','Z03.818','Z20.828','Z11.59')
  and org = 'ac'
    
union all

-- chcn
select distinct
  org,
  concat(year(start_date), '-', format_number(month(start_date),'00')) month,  
  (case
    when dx_category = 'COVID 19 Confirmed Diagnosis' then 'DIAG'
    when dx_category = 'COVID19 Suspected Infection' then 'SUS'
    when dx_category = 'COVID Exposure' then 'EXP'
    end
  ) category,
  patient_id
from
  dx
where 1=1
  and lower(dx_category) like '%covid%'
  and org = 'chcn'
union all

-- he
select distinct
  org,
  concat(year(start_date), '-', format_number(month(start_date),'00')) month,  
  (case
    when dx_category = 'COVID_DIAG' then 'DIAG'
    when dx_category = 'COVID_SUS' then 'SUS'
    when dx_category = 'COVID_EXP' then 'EXP'
    end
  ) category,
  patient_id
from
  dx
where 1=1
  and lower(dx_category) like '%covid%'
  and org = 'he'

union all

-- hcn
select distinct
  org,
  concat(year(start_date), '-', format_number(month(start_date),'00')) month,  
  (case
    when upper(code) = 'U07.1' then 'DIAG'
    when upper(code) = 'Z03.818' then 'EXP'
    when upper(code) = 'Z20.828' then 'EXP'
    when upper(code) = 'Z11.59' then 'SUS'
    end
  ) category,
  patient_id
from
  dx
where 1=1
  and org = 'hcn'
  and code is not null
  and UPPER(code) in ('U07.1','Z03.818','Z20.828','Z11.59')
);

-- COMMAND ----------

drop table if exists sdoh_housing_status;
create table sdoh_housing_status as (
  select
    org,
    patient_id,
    observation_category_nachc,
    observation_name as observation_source,
    observation_value_nachc as observation_source_value_nachc,
    (case
      when observation_name_nachc = 'homeless_flag' and lower(observation_value_nachc) = 'yes' then 'Homeless'
      when observation_name_nachc = 'homeless_flag' and lower(observation_value_nachc) = 'no' then 'Not Homeless'
      when observation_name_nachc = 'homeless_flag' and lower(observation_value_nachc) = 'unknown' then 'Unknown'
      when observation_name_nachc = 'housing_security' and lower(observation_value_nachc) = 'yes' then 'Not Homeless'
      when observation_name_nachc = 'housing_security' and lower(observation_value_nachc) = 'no' then 'Homeless'
      else observation_value_nachc
      end) observation_value_nachc
  from
    sdoh
  where 1=1
    and observation_name_nachc is not null
    and observation_name_nachc in ('homeless_flag', 'housing_security', 'housing_status')
  order by 1,2,3,4,5
);
  

-- COMMAND ----------

select distinct test_category_nachc from lab order by 1;

-- COMMAND ----------

drop table if exists patient_lab;
create table patient_lab as (
select distinct
  org,
  org_patient_id,
  patient_id,
  test_date,
  test_category_nachc,
  test_result_nachc,
  -- result
  (case
    when test_result_nachc = 'neg' then test_date
    else null
    end
  ) neg_test_date,
  (case
    when test_result_nachc = 'pos' then test_date
    else null
    end
  ) pos_test_date,
  (case
    when test_result_nachc = 'other' then test_date
    else null
    end
  ) other_test_date,
  -- test type
  (case
    when test_category_nachc = 'COVID Antibody' then test_date
    else null
    end
  ) covid_antibody_test_date,
  (case
    when test_category_nachc = 'COVID Antigen' then test_date
    else null
    end
  ) covid_antigen_test_date,
  (case
    when test_category_nachc = 'COVID Molecular' then test_date
    else null
    end
  ) covid_molecular_test_date,
  (case
    when test_category_nachc = 'COVID Molecular or Not Applicable' then test_date
    else null
    end
  ) covid_molecular_or_na_test_date,
  (case
    when test_category_nachc = 'Interpretation' then test_date
    else null
    end
  ) covid_interpretation_test_date,
  (case
    when test_category_nachc = 'Molecular or Antigen or Source' then test_date
    else null
    end
  ) covid_molecular_or_antigen_or_source_test_date,
  (case
    when test_category_nachc = 'Not Applicable' then test_date
    else null
    end
  ) covid_not_applicable_test_date,
  (case
    when test_category_nachc = 'Specimen Source' then test_date
    else null
    end
  ) covid_specimen_source_test_date,
  (case
    when test_category_nachc = 'Unknown' then test_date
    else null
    end
  ) covid_unknown_test_date
from
  lab
);

select format_number(count(*),0) total_records from patient_lab;

-- COMMAND ----------

show tables;

-- COMMAND ----------


