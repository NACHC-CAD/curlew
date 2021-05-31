-- Databricks notebook source
-- * * *
--
-- COVID BRONZE MAPPED TABLES SCRIPT 2021-04-17
-- THIS SCRIPT DOES THE MAPPINGS FROM PROVIDED VALUES TO _NACHC VALUES
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
-- DEMO_RACE_NACHC
--
-- * * *

drop table if exists demo_race_nachc;
create table demo_race_nachc as (
  select
    race,
    max(lower(race_nachc)) race_nachc
  from
    covid.demo_race_nachc
  group by 1
);

-- COMMAND ----------

-- * * *
--
-- DEMO_SEX_NACHC
--
-- * * *

drop table if exists demo_sex_nachc;
create table demo_sex_nachc as (
  select
    sex,
    max(lower(sex_nachc)) sex_nachc
  from
    covid.demo_sex_nachc
  group by 1
);

-- COMMAND ----------

-- * * *
--
-- DEMO
-- 
-- * * *

drop table if exists demo;

create table demo as (
  select distinct 
    demo.*,
    race.race_nachc,
    sex.sex_nachc
  from 
    demo_src demo
    left outer join demo_race_nachc race on lower(race.race) = lower(demo.race)
    left outer join demo_sex_nachc sex on lower(sex.sex) = lower(demo.sex) 
  where patient_id is not null
);


-- COMMAND ----------

-- * * *
--
-- ENC
-- 
-- * * *


drop table if exists enc;

create table enc as (
  select distinct * from enc_src
  where 1=1
    and patient_id is not null
    and encounter_date is not null
);


-- COMMAND ----------

-- * * *
--
-- DX
-- 
-- * * *


drop table if exists dx;

create table dx as (
  select distinct * from dx_src
);


-- COMMAND ----------

-- * * *
-- 
-- LAB_TEST_RESULT_NACHC
--
-- * * *

drop table if exists lab_test_result_nachc;
create table lab_test_result_nachc as (
  select
    test_result,
    max(test_result_nachc) test_result_nachc
  from
    covid.lab_test_result_nachc
  where 
    test_result is not null
  group by 1
);

-- COMMAND ----------

-- * * *
--
-- LAB TEST CATEGORY NACHC
--
-- * * *

-- COMMAND ----------

--
-- AC (1 of 1)
-- lab_test_category_nachc_ac_cat
--

drop table if exists lab_test_category_nachc_ac_cat;
create table lab_test_category_nachc_ac_cat as (
  select distinct
    lab.org,
    lab.test_category,
    cat_cat.test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_cat on cat_cat.test_category = lab.test_category
  where
    lab.org = 'ac'
);


-- COMMAND ----------

--
-- CHCN 
-- (1 of 2)
-- lab_test_category_nachc_chcn_code
--

drop table if exists lab_test_category_nachc_chcn_code;
create table lab_test_category_nachc_chcn_code as (
  select distinct
    lab.org,
    lab.test_code,
    cat_code.test_category_nachc test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_code on cat_code.test_code = lab.test_code
  where 1=1
    and lab.org = 'chcn'
    and lab.test_code is not null
);


-- COMMAND ----------

--
-- CHCN 
-- (2 of 2)
-- lab_test_category_nachc_chcn_name
--

drop table if exists lab_test_category_nachc_chcn_name;
create table lab_test_category_nachc_chcn_name as (
  select distinct
    lab.org,
    lab.test_name,
    cat_name.test_category_nachc test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_name on cat_name.test_name = lab.test_name
  where 1=1
    and lab.org = 'chcn'
    and lab.test_name is not null
    and cat_name.test_code is null
);


-- COMMAND ----------

--
-- HE
-- (1 of 2)
-- lab_test_category_nachc_he_code
--

drop table if exists lab_test_category_nachc_he_code;
create table lab_test_category_nachc_he_code as (
  select distinct
    lab.org,
    lab.test_code,
    cat_code.test_category_nachc test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_code on cat_code.test_code = lab.test_code
  where
    lab.org = 'he'
);

-- COMMAND ----------

--
-- HE
-- (2 of 2)
-- lab_test_category_nachc_he_cat
--

drop table if exists lab_test_category_nachc_he_cat;
create table lab_test_category_nachc_he_cat as (
  select distinct
    lab.org,
    lab.test_category,
    cat_cat.test_category_nachc test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_cat on cat_cat.test_category = lab.test_category
  where
    lab.org = 'he'
);

-- COMMAND ----------

--
-- HCN
-- (1 of 1)
-- lab_test_category_nachc_hcn_name
--

drop table if exists lab_test_category_nachc_hcn_name;
create table lab_test_category_nachc_hcn_name as (
  select distinct
    lab.org,
    lab.test_name,
    cat_name.test_category_nachc test_category_nachc
  from
    lab_src lab
    left outer join covid.lab_test_category_nachc cat_name on cat_name.test_name = lab.test_name
  where
    lab.org = 'hcn'
  order by test_category_nachc
);


-- COMMAND ----------

-- * * *
--
-- LAB
-- 
-- * * *

drop table if exists lab;
create table lab as (
  select distinct
    lab.*,
    result.test_result_nachc,
    (case
      when lab.org = 'ac' and lab.test_category is not null then ac_cat.test_category_nachc
      when lab.org = 'chcn' and lab.test_code is not null then chcn_code.test_category_nachc
      when lab.org = 'chcn' and lab.test_name is not null then chcn_name.test_category_nachc
      when lab.org = 'he' and lab.test_code is not null then he_code.test_category_nachc
      when lab.org = 'he' and lab.test_category is not null then he_cat.test_category_nachc
      when lab.org = 'hcn' and lab.test_name is not null then hcn_name.test_category_nachc
      else null
      end
    ) test_category_nachc
  from
    lab_src lab
    left outer join lab_test_result_nachc result on lower(result.test_result) = lower(lab.test_result)
    left outer join lab_test_category_nachc_ac_cat ac_cat on ac_cat.test_category = lab.test_category
    left outer join lab_test_category_nachc_chcn_code chcn_code on chcn_code.test_code = lab.test_code
    left outer join lab_test_category_nachc_chcn_name chcn_name on chcn_name.test_name = lab.test_name
    left outer join lab_test_category_nachc_he_code he_code on he_code.test_code = lab.test_code
    left outer join lab_test_category_nachc_he_cat he_cat on he_cat.test_category = lab.test_category
    left outer join lab_test_category_nachc_hcn_name hcn_name on hcn_name.test_name = lab.test_name
);

-- COMMAND ----------

-- * * *
--
-- SYMP
-- 
-- * * *

drop table if exists symp;
create table symp as (
  select distinct
    symp.*
  from
    symp_src symp
);

-- COMMAND ----------

-- * * *
--
-- SDOH
--
-- * * *

-- COMMAND ----------

drop table if exists sdoh_observation_name_nachc;
create table sdoh_observation_name_nachc as (
  select
    lower(observation_name) observation_name,
    max(lower(observation_name_nachc)) observation_name_nachc,
    max(observation_category_nachc) observation_category_nachc
  from
    covid.sdoh_observation_name_nachc
  where 1=1
    and (observation_name_nachc is not null or observation_category_nachc is not null)
  group by 1
);

-- COMMAND ----------

drop table if exists sdoh_observation_value_nachc;
create table sdoh_observation_value_nachc as (
  select
    observation_name_nachc,
    observation_value,
    max(observation_value_nachc) observation_value_nachc
  from 
    covid.sdoh_observation_value_nachc
  group by 1,2
)
;

-- COMMAND ----------

-- * * *
--
-- SDOH
--
-- * * *

drop table if exists sdoh;
create table sdoh as (
  select 
    sdoh.*,
    val_nachc.observation_value_nachc
  from
  (
    select distinct
      sdoh.*,
      obs_name_nachc.observation_name_nachc,
      obs_name_nachc.observation_category_nachc
    from
      sdoh_src sdoh
    left outer join sdoh_observation_name_nachc obs_name_nachc on lower(obs_name_nachc.observation_name) = lower(sdoh.observation_name)
  ) sdoh
  left outer join sdoh_observation_value_nachc val_nachc on val_nachc.observation_name_nachc = sdoh.observation_name_nachc 
    and val_nachc.observation_value = sdoh.observation_value 
);

-- COMMAND ----------

-- sdoh_src and sdoh should have the same number of records
select count(*) from sdoh_src
union all
select count(*) from sdoh
;
