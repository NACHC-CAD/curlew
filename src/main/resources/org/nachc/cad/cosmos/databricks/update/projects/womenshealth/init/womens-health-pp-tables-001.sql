-- Databricks notebook source
-- * * * 
--
-- INIT SESSION
-- 
-- * * *

create database if not exists womens_health_pp;
use womens_health_pp;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;


-- COMMAND ----------

-- * * *
-- 
-- BASE TABLES
--
-- * * *

refresh prj_grp_womens_health_pp_demo.Demographics;
drop table if exists demo;
create table demo using delta as select * from prj_grp_womens_health_pp_demo.Demographics where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_enc.Encounter;
drop table if exists enc;
create table enc using delta as select * from prj_grp_womens_health_pp_enc.Encounter where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_dx.dx;
drop table if exists diag;
create table diag using delta as select * from prj_grp_womens_health_pp_dx.dx where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_obs.obs;
drop table if exists obs;
create table obs using delta as select * from prj_grp_womens_health_pp_obs.obs where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_rx.rx;
drop table if exists rx;
create table rx using delta as select * from prj_grp_womens_health_pp_rx.rx where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_proc.Procedure;
drop table if exists proc;
create table proc using delta as select * from prj_grp_womens_health_pp_proc.Procedure where org_patient_id is not null;

refresh table prj_grp_womens_health_pp_proc_cat.ProcCat;
drop table if exists proc_cat;
create table proc_cat using delta as select * from prj_grp_womens_health_pp_proc_cat.ProcCat;

refresh table prj_grp_womens_health_pp_preg.Pregnancy;
drop table if exists conf_pregnancy;
create table conf_pregnancy using delta as select * from prj_grp_womens_health_pp_preg.Pregnancy;

refresh table prj_grp_womens_health_lab.lab;
drop table if exists lab;
create table lab using delta as select * from prj_grp_womens_health_lab.lab;


-- COMMAND ----------

-- * * *
-- 
-- PREGNANCY TABLE
--
-- * * *

drop table if exists pregnancy;

create table pregnancy using delta as (
-- patients with an encounter with type of '%postpartum%'
select distinct
  org,
  org_patient_id,
  patient_id,
  min(encounter_date) as est_delivery_date,
  'post_partum' as inclusion_reason,
  data_lot,
  '1' as inclusion_by_post_partum,
  null as inclusion_by_est_delivery_date_obs,
  null as inclusion_by_est_delivery_date_enc,
  null as inclusion_by_diagnosis,
  raw_table
from enc
where lower(enc_type) like '%postpartum%'
group by 1,2,3,5,6,7,8,9,10,11

union all
-- patients with an estimated delivery date in the obs table
select distinct
  obs.org,
  obs.org_patient_id,
  obs.patient_id,
  min(obs.est_delivery_date) as est_delivery_date,
  'est_delivery_date_obs' as inclusion_reason,
  obs.data_lot,
  null as inclusion_by_post_partum,
  '1' as inclusion_by_est_delivery_date_obs,
  null as inclusion_by_est_delivery_date_enc,
  null as inclusion_by_diagnosis,
  obs.raw_table
from obs join enc on obs.patient_id = enc.patient_id
where obs.est_delivery_date is not null
group by 1,2,3,5,6,7,8,9,10,11

union all
-- patients with an estimated delivery date in the enc table
select distinct
  enc.org,
  enc.org_patient_id,
  enc.patient_id,
  min(enc.est_delivery_date) as est_delivery_date,
  'est_delivery_date_enc' as inclusion_reason,
  enc.data_lot,
  null as inclusion_by_post_partum,
  null as inclusion_by_est_delivery_date_obs,
  '1' as inclusion_by_est_delivery_date_enc,
  null as inclusion_by_diagnosis,
  enc.raw_table
from enc 
where enc.est_delivery_date is not null
group by 1,2,3,5,6,7,8,9,10,11

union all
-- patients with a diagnosis of pregnancy
select distinct
  diag.org,
  diag.org_patient_id,
  diag.patient_id,
  min(date_add(coalesce(diag.dx_date, diag.enc_date, enc.encounter_date, diag.onset_date), 180)) as est_delivery_date,
  'diagnosis' as inclusion_reason,
  diag.data_lot,
  null as inclusion_by_post_partum,
  null as inclusion_by_est_delivery_date_obs,
  null as inclusion_by_est_delivery_date_enc,
  '1' as inclusion_by_diagnosis,
  diag.raw_table
from diag join enc on diag.patient_id = enc.patient_id
where lower(category) = 'pregnancy'
group by 1,2,3,5,6,7,8,9,10,11
);

-- COMMAND ----------

-- * * *
--
-- INFERTILITY
--
-- * * *

drop table if exists infertility;

create table infertility using delta as (

select distinct 
  org,
  org_patient_id,
  patient_id,
  'infertility_marker' as inclusion_reason,
  data_lot,
  '1' as inclusion_by_infertility_marker,
  null as inclusion_by_diagnosis,
  raw_table
from enc 
where infertility_marker is not null

union

select distinct 
  org,
  org_patient_id,
  patient_id,
  'diagnosis' as inclusion_reason,
  data_lot,
  null as inclusion_by_infertility_marker,
  '1' as inclusion_by_diagnosis,
  raw_table
from diag
where lower(dx_description) like '%infertil%'

);

-- COMMAND ----------

-- * * *
-- 
-- EXCLUDES FOR CONTRACEPTION
-- 
-- * * *

drop table if exists exclude_for_contraception;

create table exclude_for_contraception using delta (

-- inf
select distinct 
  org,
  org_patient_id,
  patient_id,
  'infertility' as inclusion_reason,
  data_lot,
  raw_table
from infertility

union all

-- pregnancy
select distinct 
  org,
  org_patient_id,
  patient_id,
  'pregnancy' as inclusion_reason,
  data_lot,
  raw_table
from pregnancy

union all

-- preg intention
select distinct 
  org,
  org_patient_id,
  patient_id,
  'pregnancy_intention' as inclusion_reason,
  data_lot,
  raw_table
from enc 
where lower(pregnancy_intention_marker) = 'yes'

);



-- COMMAND ----------

-- * * *
-- 
-- INCLUDES FOR CONTRACEPTION
-- 
-- * * *

drop table if exists include_for_contraception;

create table include_for_contraception using delta as (

select distinct 
  org,
  org_patient_id,
  patient_id,
  data_lot,
  raw_table
from enc 

minus

select distinct 
  org,
  org_patient_id,
  patient_id,
  data_lot,
  raw_table
from exclude_for_contraception 

);

-- COMMAND ----------

-- * * *
--
-- TERMINOLOGY AND CATEGORIZATION STUFF
-- 
-- * * *

-- COMMAND ----------

-- * * *
--
-- VALUE SET
--
-- * * *

drop table if exists value_set;

create table value_set using delta as (
select
  trim(Code) as code,
  trim(Description) as description,
  "Code Sysetm" as code_system,
  "Code System Version" as code_system_version,
  "Code System OID" as code_system_oid,
  trim(value_set_name) as value_set_name,
  trim(code_system) as value_set_code_system,
  trim(oid) as oid
from 
  value_set.value_set
);

-- COMMAND ----------

-- * * *
-- 
-- MED_DESCRIPTION_CAT
-- This table places contraception into a category based on the text description of the medication
-- 
-- * * *

drop table if exists med_description_cat;

create table med_description_cat using delta as 
select distinct
  coalesce(
    if(trim(lower(contraceptive_type)) = 'ring', 'Contraceptive Ring', null), 
    if(trim(lower(contraceptive_type)) = 'iud- copper', 'IUD-Copper', null), 
    if(trim(lower(contraceptive_type)) = 'null', null, trim(contraceptive_type))
  ) category,
  if(trim(lower(description)) = 'null', null, trim(description)) description
from
   prj_grp_womens_health_med_description_cat.MedDescriptionCat
;

-- COMMAND ----------

-- * * *
-- 
-- MED_VALUE_SET_CAT
-- This table places contraception into a category based on the VALUE SET MEMBERSHIP of the medication
-- 
-- * * *

drop table if exists med_value_set_cat;

create table med_value_set_cat using delta as (
select 
   oid,
   name,
   category,
   code_system,
   latest_version,
   author,
   status,
   steward,
   type,
   coalesce(to_date((updated_date), "yyyy-MM-dd"), to_date((updated_date), "MM/dd/yyyy"), to_date((updated_date), "yyyyMMdd")) as updated_date,
   org,
   data_lot,
   raw_table
from 
  prj_grp_womens_health_med_value_set_cat.MedValueSetCat
);

-- COMMAND ----------

-- * * *
-- 
-- PATIENT_MED_CAT
-- This table places PATIENTS into a contraception category based on the VALUE SET MEMBERSHIP then the DESCRIPTION of the medication
-- (we don't have ANY codes in this data set, only descriptions)
--
-- * * *

drop table if exists patient_med_cat;

create table patient_med_cat using delta as (
select distinct
  rx.org,
  rx.org_patient_id,
  rx.patient_id,
  rx.med_description,
  cat.category as category,
  rx.start_date as start_date
from 
  rx
  left outer join med_description_cat cat on rx.med_description = cat.description
where (rx.med_description is not null)
);


-- COMMAND ----------

-- * * *
-- 
-- PATIENT_PROC_CAT
-- 
-- * * *

drop table if exists patient_proc_cat;

create table patient_proc_cat using delta as (
select distinct 
  proc.org,
  proc.org_patient_id,
  proc.patient_id,
  proc.procedure_code,
  proc.procedure_code_description,
  coalesce(code.category, desc.category) as category,
  coalesce(code.sub_category, desc.sub_category, code.category, desc.category) as sub_category,
  proc.encounter_date as start_date
from 
  proc
  left outer join proc_cat code on proc.procedure_code = code.procedure_code
  left outer join proc_cat desc on proc.procedure_code_description = desc.procedure_code_description
where 1=1
  and (proc.procedure_code is not null or proc.procedure_code_description is not null)
);

-- COMMAND ----------

-- * * *
-- 
-- PATIENT_CONTRACEPTION_CAT
-- This table combines rx contraception and proc contraception.  
--
-- * * *

drop table if exists patient_contraception_cat;

create table patient_contraception_cat as (
select
  'procedure' as type,
  org,
  org_patient_id,
  patient_id,
  procedure_code as code,
  procedure_code_description description,
  category,
  sub_category,
  start_date
from
  patient_proc_cat
union all
select
  'medication' as type,
  org,
  org_patient_id,
  patient_id,
  null as code,
  med_description description,
  category,
  category as sub_category,
  start_date
from patient_med_cat
);

-- COMMAND ----------

-- * * *
-- 
-- DIABETES Table
-- 
-- * * *

drop table if exists diabetes;

create table diabetes as (

  select distinct 
    org, 
    org_patient_id,
    patient_id,
    dx_code,
    dx_description,
    category,
    data_lot,
    min(onset_date) onset_date,
    max(stop_date) stop_date
  from 
    diag
  where 
    category = 'Diabetes'
  group by 1,2,3,4,5,6,7
  
/*
  union 
  select distinct
    org,
    org_patient_id,
    patient_id,
    null as dx_code,
    'Diabetes Diagnosis Indicated (DM Diagnosis)' as dx_description,
    'Diabetes' as category,
    data_lot,
    min_dmdiagnosis_date onset_date,
    null as stop_date
  from
    enc
  where
    dmdiagnosis is not null
*/

  union 
  select distinct
    org,
    org_patient_id,
    patient_id,
    udsdiagnosis_of_dm as dx_code,
    null as dx_description,
    'Diabetes' as category,
    data_lot,
    dmdiagnosis_date onset_date,
    null as stop_date
  from
    enc
  where
    udsdiagnosis_of_dm is not null

  union 
  select distinct
    org,
    org_patient_id,
    patient_id,
    gestational_dm as dx_code,
    null as dx_description,
    'Gestational Diabetes' as category,
    data_lot,
    dmdiagnosis_date onset_date,
    null as stop_date
  from
    enc
  where
    gestational_dm is not null
  union

  select distinct
    diag.org, 
    diag.org_patient_id,
    diag.patient_id,
    diag.dx_code,
    diag.dx_description,
    'Gestational Diabetes' as category,
    diag.data_lot,
    min(diag.onset_date) onset_date,
    max(diag.stop_date) stop_date
  from 
    diag
    join enc on diag.patient_id = enc.patient_id
  where 1=1
    and (
      (lower(coalesce(dx_description, category)) like '%gest%' and lower(coalesce(dx_description, category)) like '%diab%')
      or lower(dx_code) like 'o24.41%'
      or lower(gestational_dm) like 'o24.41%'
    )  
  group by 1,2,3,4,5,6,7

);

-- COMMAND ----------


-- * * *
-- 
-- GESTATIONAL DIABETES
-- 
-- * * *

drop table if exists gest_diab;

create table gest_diab using delta as (
select distinct
  diag.org,
  diag.org_patient_id,
  diag.patient_id,
  dx_code,
  gestational_dm as dm_code,
  coalesce(dx_description, category) description,
  diag.enc_date,
  diag.dx_date,
  diag.onset_date,
  diag.stop_date,
  diag.data_lot
from 
  diag
  join enc on diag.patient_id = enc.patient_id
where 1=1
  and (
    (lower(coalesce(dx_description, category)) like '%gest%' and lower(coalesce(dx_description, category)) like '%diab%')
    or lower(dx_code) like 'o24.41%'
    or lower(gestational_dm) like 'o24.41%'
  )
);


-- COMMAND ----------


-- * * *
-- 
-- PERMISSIONS
-- 
-- * * *

grant usage, select on database womens_health_pp to `users`;

