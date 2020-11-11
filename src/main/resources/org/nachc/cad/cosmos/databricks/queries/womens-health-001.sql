-- Databricks notebook source
create database womens_health;

use womens_health;


-- COMMAND ----------

-- 
-- refresh base tables (TODO: Automate this)
-- 

refresh table prj_grp_womens_health_demo.Demographics;
refresh table prj_grp_womens_health_dx.Diagnosis;
refresh table prj_grp_womens_health_enc.Encounter;
refresh table prj_grp_womens_health_fert.Fertility;
refresh table prj_grp_womens_health_obs.Observation;
refresh table prj_grp_womens_health_other.Other;
refresh table prj_grp_womens_health_proc.Procedure;
refresh table prj_grp_womens_health_rx.Rx;


-- COMMAND ----------

--
-- create the src tables
--

use womens_health;

create table demo_src as select * from prj_grp_womens_health_demo.Demographics;
create table enc_src as select * from prj_grp_womens_health_enc.Encounter;
create table diag_src as select * from prj_grp_womens_health_dx.Diagnosis;
create table fert_src as select * from prj_grp_womens_health_fert.Fertility;
create table obs_src as select * from prj_grp_womens_health_obs.Observation;
create table other_src as select * from prj_grp_womens_health_other.Other;
create table proc_src as select * from prj_grp_womens_health_proc.Procedure;
create table rx_src as select * from prj_grp_womens_health_rx.Rx;

-- COMMAND ----------

-- 
-- simple tables
--

create table demo as select * from prj_grp_womens_health_demo.Demographics where patient_id is not null;
create table diag as select * from prj_grp_womens_health_dx.Diagnosis where patient_id is not null;


-- COMMAND ----------

--
-- encounter stuff
-- 

-- COMMAND ----------

--
-- enc_dup_dates
--
-- This table has the roll ups in it but still contains some dups on encounter date.  
--

set spark.sql.legacy.timeParserPolicy = LEGACY;

create table enc_dup_dates as (
select distinct
  patient_id,
  encounter_id,
  encounter_date encounter_date_string, 
  coalesce(to_date(encounter_date, "yyyy-MM-dd"), to_date(encounter_date, "MM/dd/yyyy"), to_date(encounter_date, "yyyyMMdd")) as encounter_date,
  enc_type,
  health_center_id,
  org,
  raw_table,
  max(if(lower(pregnancy_intention) = 'yes', 1, 0)) pregnancy_intention_yes,
  max(if(lower(pregnancy_intention) = 'no', 1, 0)) pregnancy_intention_no,
  max(if(lower(pregnancy_intention) = 'ok either way', 1, 0)) pregnancy_intention_either,
  max(if(lower(pregnancy_intention) = 'unsure', 1, 0)) pregnancy_intention_unsure,
  max(if(lower(contraceptive_counseling) = 'contraception counseling' or lower(contraceptive_counseling) = 'yes', 1, 0)) contraceptive_counseling,
  max(if(lower(infertility_marker) = 'null' or infertility_marker is null, 0, 1)) infertility_marker,
  max(if(lower(pregnancy_marker) = 'null' or pregnancy_marker is null, 0, 1)) pregnancy_marker,
  max(if(lower(pregnancy_termination_marker) = 'null' or pregnancy_termination_marker is null, 0, 1)) pregnancy_termination_marker,
  max(if(lower(sexually_active) in ('never', 'not currently'), 0, 1)) sexually_active,
  max(if(lower(dmdiagnosis) in ('yes'), 1, 0)) dmdiagnosis,
  -- dmdiagnosis_date
  max(dmdiagnosis_date) max_dmdiagnosis_date_string,
  min(dmdiagnosis_date) min_dmdiagnosis_date_string,
  coalesce(to_date(max(dmdiagnosis_date), "yyyy-MM-dd"), to_date(max(dmdiagnosis_date), "MM/dd/yyyy"), to_date(max(dmdiagnosis_date), "yyyyMMdd")) as max_dmdiagnosis_date,
  coalesce(to_date(min(dmdiagnosis_date), "yyyy-MM-dd"), to_date(min(dmdiagnosis_date), "MM/dd/yyyy"), to_date(min(dmdiagnosis_date), "yyyyMMdd")) as min_dmdiagnosis_date,
  -- delivery_date
  max(delivery_date) max_delivery_date_string,
  min(delivery_date) min_delivery_date_string,
  coalesce(to_date(max(delivery_date), "yyyy-MM-dd"), to_date(max(delivery_date), "MM/dd/yyyy"), to_date(max(delivery_date), "yyyyMMdd")) as max_delivery_date,
  coalesce(to_date(min(delivery_date), "yyyy-MM-dd"), to_date(min(delivery_date), "MM/dd/yyyy"), to_date(min(delivery_date), "yyyyMMdd")) as min_delivery_date,
  -- est_delivery_date
  max(est_delivery_date) max_est_delivery_date_string,
  min(est_delivery_date) min_est_delivery_date_string,
  coalesce(to_date(max(est_delivery_date), "yyyy-MM-dd"), to_date(max(est_delivery_date), "MM/dd/yyyy"), to_date(max(est_delivery_date), "yyyyMMdd")) as max_est_delivery_date,
  coalesce(to_date(min(est_delivery_date), "yyyy-MM-dd"), to_date(min(est_delivery_date), "MM/dd/yyyy"), to_date(min(est_delivery_date), "yyyyMMdd")) as min_est_delivery_date
from enc_src
where 1 = 1
  and patient_id is not null
  and encounter_id is not null
group by 1,2,3,4,5,6,7,8
)
;

-- COMMAND ----------

-- 
-- enc
-- This table gets rid of the dup dates in the enc_dup_dates table.
-- 

create table enc as (
  select * from enc_dup_dates
  where (patient_id, encounter_id) not in (
    select patient_id, encounter_id from (
      select 
        patient_id, encounter_id, count(*) cnt 
      from enc_dup_dates
      group by 1,2
      having cnt > 1
    )
  )
);

-- COMMAND ----------

-- 
-- est_delivery_date_obs
-- 

set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists est_delivery_date_obs;

create table est_delivery_date_obs as (
select
  org,
  encounter_id,
  -- delivery_date
  min(delivery_date) min_delivery_date_string,
  max(delivery_date) max_delivery_date_string,
  coalesce(to_date(max(delivery_date), "yyyy-MM-dd"), to_date(max(delivery_date), "MM/dd/yyyy"), to_date(max(delivery_date), "yyyyMMdd")) max_delivery_date,
  coalesce(to_date(min(delivery_date), "yyyy-MM-dd"), to_date(min(delivery_date), "MM/dd/yyyy"), to_date(min(delivery_date), "yyyyMMdd")) min_delivery_date,
  -- est_delivery_date
  min(est_delivery_date) min_est_delivery_date_string,
  max(est_delivery_date) max_est_delivery_date_string,
  coalesce(to_date(max(est_delivery_date), "yyyy-MM-dd"), to_date(max(est_delivery_date), "MM/dd/yyyy"), to_date(max(est_delivery_date), "yyyyMMdd")) max_est_delivery_date,
  coalesce(to_date(min(est_delivery_date), "yyyy-MM-dd"), to_date(min(est_delivery_date), "MM/dd/yyyy"), to_date(min(est_delivery_date), "yyyyMMdd")) min_est_delivery_date
from
  obs_src
group by 1,2
)
;


-- COMMAND ----------

--
-- est_delivery_date
-- 

use womens_health;

set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists est_delivery_date;

create table est_delivery_date as (
select
  enc.org,
  enc.encounter_id,
  enc.patient_id,
  enc.encounter_date,
  coalesce(enc.max_est_delivery_date, obs.max_est_delivery_date) max_est_delivery_date,
  coalesce(enc.min_est_delivery_date, obs.min_est_delivery_date) min_est_delivery_date
from
  enc
  left outer join est_delivery_date_obs obs on obs.org = enc.org and obs.encounter_id = enc.encounter_id
)
;

-- COMMAND ----------

-- 
-- pregnancy
-- 

drop table if exists pregnancy;
create table pregnancy using delta as (
-- post partum visit type
select distinct patient_id, org, cast(null as string) post_partum, cast(null as string) diag, cast(null as string) est_delivery_date 
from enc 
where lower(enc_type) like '%postpartum%'
union 
-- pregnancy diagnosis
select distinct patient_id, org, cast(null as string) post_partum, cast(null as string) diag, cast(null as string) est_delivery_date 
from diag 
where lower(category) = 'pregnancy'
union
-- visit with due date
select distinct enc.patient_id, org, cast(null as string) post_partum, cast(null as string) diag, cast(null as string) est_delivery_date 
from enc 
where max_est_delivery_date is not null
) 
;

-- COMMAND ----------

-- 
-- births (create table)
-- 

drop table if exists birth;

create table birth using delta as 
select org, patient_id, cast(null as string) postpartum_enc_type, cast(null as string) postpartum_enc, cast(null as string) postpartum_dx 
from (
-- post partum encounter type  
select distinct org, patient_id from enc where lower(enc_type) like '%postpartum%'
-- encounter between due date and 14 days after due date
union
select 
  distinct enc.org, enc.patient_id
from
  enc,
  (select patient_id, max_est_delivery_date est_date from enc) est
where 1=1
  and enc.patient_id = est.patient_id 
  and enc.encounter_date >= est.est_date 
  and enc.encounter_date <  date_add(est.est_date, 14)
-- diagnosis with post partum indication
union
select distinct org, patient_id
from diag 
where 1=1
  and lower(dx_description) like '%postpartum%'
)
;



-- COMMAND ----------

-- 
-- infertility stuff
-- 

-- COMMAND ----------

drop table if exists inf;

create table inf using delta as (
select distinct org, patient_id, cast(null as string) inf_marker, cast(null as string) inf_diag 
from enc 
where infertility_marker = 1
union all 
select distinct org, patient_id, cast(null as string) inf_marker, cast(null as string) inf_diag 
from diag
where lower(dx_description) like '%infertil%'
)
;

update inf set inf_marker = '1' where patient_id in (
  select distinct patient_id
  from enc 
  where infertility_marker = 1
);

update inf set inf_diag = '1' where patient_id in (
  select distinct patient_id
  from diag
  where lower(dx_description) like '%infertil%'
);

