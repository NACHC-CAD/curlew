-- Databricks notebook source
-- * * *
--
-- COVID BRONZE PATIENT STAR SCHEMA
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- INIT THE SESSION
--
-- * * *

create database if not exists covid_bronze_pat_star;
use covid_bronze_pat_star;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;
select 'USING COVID_BRONZE_PAT_STAR';

-- COMMAND ----------

-- * * *
--
-- CREATE TABLES
--
-- * * *

-- COMMAND ----------

-- * * *
--
-- PATIENT
-- (This is the center of the star schema)
--
-- * * *

drop table if exists patient;
create table patient as (
  select distinct 
    org, 
    org_patient_id,
    patient_id
  from covid_bronze.demo
);

select count(*) from patient;

-- COMMAND ----------

-- * * *
--
-- DEMO
-- Demographics dimention
--
-- * * *

drop table if exists demo;
create table demo as (
  select
    demo.org,
    demo.org_patient_id,
    demo.patient_id,
    cast (last(demo.year_of_birth) as integer) year_of_birth,
    last(demo.sex_nachc) sex,
    last(demo.race) race,
    last(demo.ethnicity) ethnicity,
    last(demo.patient_race) race_eth,
    last(demo.zip) zip,
    last(demo.state) state
  from (
    select 
      row_number() over (order by provided_date, data_lot) as row_num, 
      demo.*,
      race.race as patient_race
    from
      covid_bronze.demo demo
      left outer join covid_bronze.patient_race race on race.patient_id = demo.patient_id
    order by 1
  ) demo
  group by 1,2,3
);

-- COMMAND ----------

-- * * *
--
-- ENC
-- Encounters Dimention
--
-- * * *

drop table if exists enc;
create table enc as (
select
  org,
  org_patient_id,
  patient_id,
  coalesce(count(distinct encounter_date),0) number_of_encounters,
  min(encounter_date) first_encounter_date,
  max(encounter_date) last_encounter_date
from
  covid_bronze.enc
group by 1,2,3
);

select count(*) from enc
union all 
select count(distinct patient_id) from enc;

-- COMMAND ----------

-- * * *
--
-- EXP/SUS/DIAG TABLES
-- 
-- * * *

-- COMMAND ----------

drop table if exists patient_covid_obs_exp;
create table patient_covid_obs_exp as (
select distinct
  org,
  org_patient_id,
  patient_id,
  category,
  min(obs_date) first_obs_date,
  max(obs_date) last_obs_date
from
  covid_bronze.patient_covid_obs
where 1=1
  and category = 'EXP'
group by 1,2,3,4
);

-- COMMAND ----------

drop table if exists patient_covid_obs_sus;
create table patient_covid_obs_sus as (
select distinct
  org,
  org_patient_id,
  patient_id,
  category,
  min(obs_date) first_obs_date,
  max(obs_date) last_obs_date
from
  covid_bronze.patient_covid_obs
where 1=1
  and category = 'SUS'
group by 1,2,3,4
);

-- COMMAND ----------

drop table if exists patient_covid_obs_diag;
create table patient_covid_obs_diag as (
select distinct
  org,
  org_patient_id,
  patient_id,
  category,
  min(obs_date) first_obs_date,
  max(obs_date) last_obs_date
from
  covid_bronze.patient_covid_obs
where 1=1
  and category = 'DIAG'
group by 1,2,3,4
);

-- COMMAND ----------

-- * * *
--
-- LAB
-- 
-- * * *

drop table if exists lab;
create table lab as (
select
  org,
  org_patient_id,
  patient_id,
  -- tests
  coalesce(count(distinct test_date),0) number_of_tests,
  -- tests (pos/neg/other)
  coalesce(count(distinct neg_test_date),0) number_of_pos_tests,
  coalesce(count(distinct pos_test_date),0) number_of_neg_tests,
  coalesce(count(distinct other_test_date),0) number_of_other_tests,
  (case
    when max(pos_test_date) is null then 0
    else 1
    end
  ) has_pos_test,
  -- has had test by type
  (case
    when count(distinct covid_antibody_test_date) = 0 then 0
    else 1
    end
  ) has_antibody_test,
  (case
    when count(distinct covid_antigen_test_date)  = 0 then 0
    else 1
    end
  ) has_antigent_test,
  (case
   
    when count(distinct covid_molecular_test_date) = 0 then 0
    else 1
    end
  ) has_molecular_test,
  (case
    when count(distinct covid_molecular_or_na_test_date) = 0 then 0
    else 1
    end
  ) has_molecular_or_na_test,
  (case
    when count(distinct covid_interpretation_test_date) = 0 then 0
    else 1
    end
  ) has_interpretation_test,
  (case
    when count(distinct covid_molecular_or_antigen_or_source_test_date) = 0 then 0
    else 1
    end
  ) has_mol_anti_source_test,
  (case
    when count(distinct covid_not_applicable_test_date) = 0 then 0
    else 1
    end
  ) has_na_test,
  (case
    when count(distinct covid_specimen_source_test_date)  = 0 then 0
    else 1
    end
  ) has_specimen_source_test,
  (case
    when count(distinct covid_unknown_test_date) = 0 then 0
    else 1
    end
  ) has_unknown_test,
  -- counts by test type
  coalesce(count(distinct covid_antibody_test_date),0) antibody_test_cnt,
  coalesce(count(distinct covid_antigen_test_date),0) antigent_test_cnt,
  coalesce(count(distinct covid_molecular_test_date),0) molecular_test_cnt,
  coalesce(count(distinct covid_molecular_or_na_test_date),0) molecular_or_na_test_cnt,
  coalesce(count(distinct covid_interpretation_test_date),0) interpretation_test_cnt,
  coalesce(count(distinct covid_molecular_or_antigen_or_source_test_date),0) mol_anti_source_test_cnt,
  coalesce(count(distinct covid_not_applicable_test_date),0) na_test_cnt,
  coalesce(count(distinct covid_specimen_source_test_date),0) specimen_source_test_cnt,
  coalesce(count(distinct covid_unknown_test_date),0) unknown_test_cnt,
  -- firsts and lasts
  min(test_date) first_test,
  max(test_date) last_test,
  min(pos_test_date) first_pos_test,
  max(pos_test_date) last_pos_test,
  min(neg_test_date) first_neg_test,
  max(neg_test_date) last_neg_test,
  -- sets and lists
  concat_ws(",",collect_set(test_result_nachc)) test_result_set,
  concat_ws(",",collect_list(coalesce(test_result_nachc, 'null'))) test_result_list,
  concat_ws(",",collect_set(test_date)) test_date_set,
  concat_ws(",",collect_list(coalesce(test_date, 'null'))) test_date_list
from (
    select
      row_number() over (order by test_date) row_number,
      lab.*
    from
      covid_bronze.patient_lab lab
    order by test_date
  ) lab
group by 1,2,3
);

-- COMMAND ----------

-- * * *
--
-- PATIENT FACT TABLE
--
-- * * *

drop table if exists patient_fact;
create table patient_fact as (
select
  -- patient
  pat.*,
  -- demo
  demo.year_of_birth,
  demo.sex,
  demo.race,
  demo.ethnicity,
  demo.race_eth,
  demo.zip,
  demo.state,
  -- enc
  enc.number_of_encounters,
  enc.first_encounter_date,
  enc.last_encounter_date,
  -- exp
  exp.first_obs_date as first_exp_date,
  exp.last_obs_date as last_exp_date,
  (case
    when exp.first_obs_date is null then 0
    else 1
    end
  ) has_covid_exp,
  -- sus
  sus.first_obs_date as first_sus_date,
  sus.last_obs_date as last_sus_date,
  (case
    when sus.first_obs_date is null then 0
    else 1
    end
  ) has_covid_sus,
  -- diag
  diag.first_obs_date as first_diag_date,
  diag.last_obs_date as last_diag_date,
  (case
    when diag.first_obs_date is null then 0
    else 1
    end
  ) has_covid_diag,
  -- lab
  lab.number_of_tests,
  lab.number_of_pos_tests,
  lab.number_of_neg_tests,
  lab.number_of_other_tests,
  lab.has_pos_test,
  lab.has_antibody_test,
  lab.has_antigent_test,
  lab.has_molecular_test,
  lab.has_molecular_or_na_test,
  lab.has_interpretation_test,
  lab.has_mol_anti_source_test,
  lab.has_na_test,
  lab.has_specimen_source_test,
  lab.has_unknown_test,
  lab.antibody_test_cnt,
  lab.antigent_test_cnt,
  lab.molecular_test_cnt,
  lab.molecular_or_na_test_cnt,
  lab.interpretation_test_cnt,
  lab.mol_anti_source_test_cnt,
  lab.na_test_cnt,
  lab.specimen_source_test_cnt,
  lab.unknown_test_cnt,
  lab.first_test,
  lab.last_test,
  lab.first_pos_test,
  lab.last_pos_test,
  lab.first_neg_test,
  lab.last_neg_test,
  lab.test_result_set,
  lab.test_result_list,
  lab.test_date_set,
  lab.test_date_list
from
  patient pat
  left outer join demo on demo.patient_id = pat.patient_id
  left outer join enc on enc.patient_id = pat.patient_id
  left outer join patient_covid_obs_exp exp on exp.patient_id = pat.patient_id
  left outer join patient_covid_obs_sus sus on sus.patient_id = pat.patient_id
  left outer join patient_covid_obs_diag diag on diag.patient_id = pat.patient_id
  left outer join lab on lab.patient_id = pat.patient_id
);

select 'patient' table, count(*) total from patient
union all 
select 'patient_fact' table, count(*) total from patient_fact
;

-- COMMAND ----------

describe patient_fact;

-- COMMAND ----------

select count(distinct patient_id) from covid_bronze.demo
union all
select count(*) from patient_fact;

-- COMMAND ----------

select sum(number_of_tests) from patient_fact;

