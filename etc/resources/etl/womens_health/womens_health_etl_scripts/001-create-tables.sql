-- Databricks notebook source
-- * * *
-- 
-- (2021-04-28)
-- UPDATED CREATE TABLES FOR WOMENS HEALTH SCHEMA
-- 2021-01-31: Updated to use merge functionality.  
-- 
-- * * *

-- COMMAND ----------

-- * * * 
--
-- INIT SESSION
-- 
-- * * *

create database if not exists womens_health;
use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
-- 
-- BASE TABLES
--
-- * * *


-- COMMAND ----------

refresh prj_grp_womens_health_demo.demographics;
drop table if exists demo;
create table demo using delta as select * from prj_grp_womens_health_demo.demographics where org_patient_id is not null;

refresh table prj_grp_womens_health_enc.encounter;
drop table if exists enc;
create table enc using delta as select * from prj_grp_womens_health_enc.encounter where org_patient_id is not null;

refresh table prj_grp_womens_health_dx.diagnosis;
drop table if exists diag;
create table diag using delta as select * from prj_grp_womens_health_dx.diagnosis where org_patient_id is not null;

refresh table prj_grp_womens_health_rx.rx;
drop table if exists rx;
create table rx using delta as select * from prj_grp_womens_health_rx.rx where org_patient_id is not null;

refresh table prj_grp_womens_health_proc.Procedure;
drop table if exists proc;
create table proc using delta as select * from prj_grp_womens_health_proc.Procedure where org_patient_id is not null;

refresh table prj_grp_womens_health_obs.observation;
drop table if exists obs;
create table obs using delta as select * from prj_grp_womens_health_obs.observation where org_patient_id is not null;

refresh table prj_grp_womens_health_proc_cat.ProcCat;
drop table if exists proc_cat;
create table proc_cat using delta as select * from prj_grp_womens_health_proc_cat.ProcCat;

refresh table prj_grp_womens_health_preg.Pregnancy;
drop table if exists conf_pregnancy;
create table conf_pregnancy using delta as select * from prj_grp_womens_health_preg.Pregnancy;

refresh table prj_grp_womens_health_lab.lab;
drop table if exists lab;
create table lab using delta as select * from prj_grp_womens_health_lab.lab;

refresh table prj_grp_womens_health_flat.flatfile;
drop table if exists flat;
create table flat using delta as select * from prj_grp_womens_health_flat.flatfile where patient_id is not null;

refresh table prj_grp_womens_health_other.other;
drop table if exists other;
create table other using delta as select * from prj_grp_womens_health_other.other where patient_id is not null;


-- COMMAND ----------

-- * * *
--
-- VALUE_SET
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
-- MERGE DATA FROM FLAT INTO BASE TABLES
--
-- * * *

-- COMMAND ----------

-- * * *
-- 
-- MERGE:
-- DEMO (Demographics) from FLAT
-- 
-- * * *

merge 
into demo
using (
  select distinct
    age,
    education,
    ethnicity,
    coalesce(fplvalue, fplrange) as fpl,
    gender_identity,
    housing_situation_uds,
    language,
    org_patient_id,
    race,
    sexat_birth,
    coalesce(insurance_primary_payer, insurance_financial_class) as insurance,
    most_recent_encounter_location,
    orientation,
    orientation,
    orientation,
    patient_id,
    org,
    data_lot,
    raw_table
  from flat
  where patient_id is not null
)
on 0=1
when not matched then 
insert (
  age,
  education,
  ethnicity,
  fpl,
  gender_identity,
  housing_status,
  language,
  org_patient_id,
  race,
  sex,
  insurance,
  health_center_id,
  sexual_orientation_psot,
  sexual_orientation_sd,
  sexual_orientation_sochist,
  patient_id,
  org,
  data_lot,
  raw_table
)
values (
  age,
  education,
  ethnicity,
  fpl,
  gender_identity,
  housing_situation_uds,
  language,
  org_patient_id,
  race,
  sexat_birth,
  insurance,
  most_recent_encounter_location,
  orientation,
  orientation,
  orientation,
  patient_id,
  org,
  data_lot,
  raw_table
)
;

-- COMMAND ----------

-- * * *
--
-- MERGE:
-- ENC (Encounter) from FLAT
-- 
-- * * *

merge into enc
using (
  select distinct
    most_recent_encounter_date as encounter_date,
    most_recent_encounter_date as encounter_id,
    org_patient_id as org_patient_id,
    sexual_hx_detail as sexually_active,
    infecund_code as infertility_marker,
    if(pregnancy_est_delivery is not null, 1, null) as pregnancy_marker,
    coalesce(pregnancy_termination_code, fetal_demise_code, spontaneous_abortion_code) as pregnancy_termination_marker,
    flat.contraceptive_counseling as contraceptive_counseling,
    most_recent_encounter_location as health_center,
    pregnancy_est_delivery as max_est_delivery_date,
    pregnancy_est_delivery as min_est_delivery_date,
    lower(pisqresponse) as pregnancy_intention_marker,
    contraceptive_counseling,
    patient_id as patient_id,
    org as org,
    data_lot as data_lot,
    raw_table as raw_table
  from 
    womens_health.flat flat
  where flat.patient_id is not null
)
on 0=1
when not matched then
insert (
    encounter_date,
    encounter_id,
    org_patient_id,
    sexually_active,
    infertility_marker,
    pregnancy_marker,
    pregnancy_termination_marker,
    contraceptive_counseling,
    health_center,
    max_est_delivery_date,
    min_est_delivery_date,
    pregnancy_intention_marker,
    patient_id,
    org,
    data_lot,
    raw_table
)
values (
    encounter_date,
    encounter_id,
    org_patient_id,
    sexually_active,
    infertility_marker,
    pregnancy_marker,
    pregnancy_termination_marker,
    flat.contraceptive_counseling,
    health_center,
    max_est_delivery_date,
    min_est_delivery_date,
    pregnancy_intention_marker,
    patient_id,
    org,
    data_lot,
    raw_table
)
;


-- COMMAND ----------

-- * * *
-- 
-- MERGE
-- DIAG (Diagnosis) from FLAT
-- 
-- * * *

merge into diag
using (
	select distinct 
	  org_patient_id as org_patient_id, 
	  larcmethod_code as dx_code, 
	  most_recent_encounter_location as health_center_id, 
	  most_recent_encounter_date as enc_date, 
	  most_recent_encounter_date as encounter_id, 
	  patient_id as patient_id, 
	  org as org, 
	  data_lot as data_lot, 
	  raw_table as raw_table
	from 
	  womens_health.flat flat
	where flat.patient_id is not null
)
on 0=1
when not matched then 
insert (
    org_patient_id, 
    dx_code, 
    health_center_id, 
    enc_date, 
    encounter_id, 
    patient_id, 
    org, 
    data_lot, 
    raw_table
)
values (
    org_patient_id, 
    dx_code, 
    health_center_id, 
    enc_date, 
    encounter_id, 
    patient_id, 
    org, 
    data_lot, 
    raw_table
)


-- COMMAND ----------

-- * * *
--
-- MERGE
-- RX (Prescriptions) from FLAT
-- 
-- * * *

merge into rx
using (
	select distinct
	  most_recent_encounter_date as encounter_date, 
	  most_recent_encounter_date as encounter_id, 
	  contraceptive_rx_name as med_description, 
	  org_patient_id as org_patient_id, 
	  contraceptive_rx_refills as refills, 
	  most_recent_encounter_location as health_center, 
	  patient_id as patient_id, 
	  org as org, 
	  data_lot as data_lot, 
	  raw_table as raw_table
	from 
	  womens_health.flat flat
	where 1=1
      and flat.patient_id is not null
      and flat.contraceptive_rx_name is not null
)
on 0=1
when not matched then 
insert (
  encounter_date, 
  encounter_id, 
  med_description, 
  org_patient_id, 
  refills, 
  health_center, 
  patient_id, 
  org, 
  data_lot, 
  raw_table
)
values (
  encounter_date, 
  encounter_id, 
  med_description, 
  org_patient_id, 
  refills, 
  health_center, 
  patient_id, 
  org, 
  data_lot, 
  raw_table
)
;


-- COMMAND ----------

-- * * * 
-- 
-- MERGE
-- PROC (Procedures) from FLAT
-- 
-- * * *

merge into proc
using (
	select distinct 
	  larcmethod_code as procedure_code, 
	  most_recent_encounter_location as health_center_id, 
	  patient_id as patient_id, 
	  org as org, 
	  data_lot as data_lot, 
	  raw_table as raw_table
	from 
	  womens_health.flat flat
	where 
	  flat.patient_id is not null
)
on 0=1
when not matched then 
insert (
  procedure_code, 
  health_center_id, 
  patient_id, 
  org, 
  data_lot, 
  raw_table
)
values (
  procedure_code, 
  health_center_id, 
  patient_id, 
  org, 
  data_lot, 
  raw_table
)
;


-- COMMAND ----------

-- * * *
-- 
-- DERIVED TABLES
--
-- * * *

-- COMMAND ----------

-- * * *
-- 
-- PREGNANCY INTENTION TABLE
-- 
-- * * *

drop table if exists pregnancy_intention;
create table pregnancy_intention as (
select distinct
  enc.org,
  enc.org_patient_id,
  enc.patient_id,
  enc.pregnancy_intention_marker,
  enc.data_lot,
  enc.raw_table
from
  enc
union
select distinct
  obs.org,
  obs.org_patient_id,
  obs.patient_id,
  obs.pregnancy_intention as pregnancy_intention_marker,
  obs.data_lot,
  obs.raw_table
from
  obs
)
;

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
  raw_table
from enc
where lower(enc_type) like '%postpartum%'
group by 1,2,3,5,6,7

union 
-- patients with an estimated delivery date in the obs table
select distinct
  obs.org,
  obs.org_patient_id,
  obs.patient_id,
  min(obs.est_delivery_date) as est_delivery_date,
  'est_delivery_date_obs' as inclusion_reason,
  obs.data_lot,
  obs.raw_table
from obs join enc on obs.patient_id = enc.patient_id
where obs.est_delivery_date is not null
group by 1,2,3,5,6,7

union 
-- patients with an estimated delivery date in the enc table
select distinct
  enc.org,
  enc.org_patient_id,
  enc.patient_id,
  min(enc.est_delivery_date) as est_delivery_date,
  'est_delivery_date_enc' as inclusion_reason,
  enc.data_lot,
  enc.raw_table
from enc 
where enc.est_delivery_date is not null
group by 1,2,3,5,6,7

union 
-- patients with a diagnosis of pregnancy
select distinct
  diag.org,
  diag.org_patient_id,
  diag.patient_id,
  -- date_add(min(coalesce(diag.diagnosis_date, diag.encounter_date, enc.encounter_date, diag.onset_date)), 180) birth_day
  date_add(min(coalesce(diag.onset_date, diag.dx_date, diag.enc_date)), 180) as est_delivery_date,
  'diagnosis' as inclusion_reason,
  diag.data_lot,
  diag.raw_table
from diag join enc on diag.patient_id = enc.patient_id
where lower(category) = 'pregnancy'
group by 1,2,3,5,6,7

union 
-- health efficiency pregnancies
select 
  enc.org,
  enc.org_patient_id,
  enc.patient_id,
  coalesce(max_act_delivery_date, min_act_delivery_date, max_est_delivery_date, min_est_delivery_date) as est_delivery_date,
  'pregnancy_marker_he' as inclusion_reason,
  enc.data_lot,
  enc.raw_table
from
  enc
where 1=1
and (
  min_act_delivery_date is not null
  or max_act_delivery_date is not null
  or min_est_delivery_date is not null
  or max_est_delivery_date is not null
  or current_pregnancy = 1
  or preg_term_marker = 1
)

union
-- sc patients
select 
  flat.org,
  flat.org_patient_id,
  flat.patient_id,
  pregnancy_est_delivery as est_delivery_date,
  'pregnancy_marker_sc' as inclusion_reason,
  flat.data_lot,
  flat.raw_table
from
  flat
where 1=1
and (
  birthweight_date is not null
  or birthweight_detail is not null
  or fetal_demise_code is not null
  or fetal_demise_date is not null
  or pregnancy_edd is not null
  or pregnancy_est_delivery is not null
  or pregnancy_termination_code is not null
  or pregnancy_termination_date is not null
  or spontaneous_abortion_code is not null
  or spontaneous_abortion_date is not null
)

);

-- COMMAND ----------

-- * * *
--
-- INFERTILITY
--
-- * * *

drop table if exists infertility;

create table infertility using delta as (

-- infertility marker in enc
select distinct 
  org,
  org_patient_id,
  patient_id,
  'infertility_marker' as inclusion_reason,
  data_lot,
  raw_table
from enc 
where 1=1 
  and infertility_marker is not null
  and infertility_marker != '0'

union
-- infertility diagnosis
select distinct 
  org,
  org_patient_id,
  patient_id,
  'diagnosis' as inclusion_reason,
  data_lot,
  raw_table
from diag
where lower(dx_description) like '%infertil%'

union
-- infecund from flat
select distinct 
  org,
  org_patient_id,
  patient_id,
  'infecund_from_flat' as inclusion_reason,
  data_lot,
  raw_table
from flat
where infecund_code is not null

);

-- COMMAND ----------

select * from other;

-- COMMAND ----------

-- * * * 
-- 
-- HYSTERECTOMY
-- 
-- * * *

drop table if exists hyst;

create table hyst as (
select 
  org,
  org_patient_id,
  patient_id,
  'hysterectomy_other' as inclusion_reason,
  data_lot,
  raw_table
from other
where 1=1
  and (
    lower(contraceptive_documented1__lmp) like '%hysterectomy%'
    or lower(contraceptive_documented2__shbcp) like '%hysterectomy%'
    or lower(contraceptive_documented3__sde) like '%hysterectomy%'
    or lower(contraceptive_documented_code4) like '%hysterectomy%'
    or lower(contraceptive_documented4__icd10) like '%hysterectomy%'
    or lower(contraceptive_documented_code5) like '%hysterectomy%'
    or lower(contraceptive_documented5_dx) like '%hysterectomy%'
  )
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
-- hyst
select distinct
  org,
  org_patient_id,
  patient_id,
  'hysterectomy' as inclusion_reason,
  data_lot,
  raw_table
from hyst

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
from pregnancy_intention 
where lower(pregnancy_intention_marker) in ('yes','y')

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
    patient_id
  from 
    enc 
  minus
  select distinct 
    patient_id
  from 
    exclude_for_contraception 
);

-- COMMAND ----------

drop table if exists counseling;
create table counseling as (
select distinct
  org,
  org_patient_id,
  patient_id,
  encounter_id,
  encounter_date,
  coalesce(encounter_type, enc_type) encounter_type,
  contraceptive_counseling,
  data_lot
from
  enc
where lower(contraceptive_counseling) = 'y'
  or lower(contraceptive_counseling) = 'yes'
  or lower(contraceptive_counseling) like 'z30%'
  or contraceptive_counseling = 1
);


-- COMMAND ----------

-- * * *
-- 
-- INCLUDES FOR CONTRACEPTION WITH COUNSELING
-- 
-- * * *

drop table if exists include_for_contraception_with_counseling;

create table include_for_contraception_with_counseling as (
  select 
    couns.*
  from
    include_for_contraception inc
    join counseling couns on couns.patient_id = inc.patient_id
);

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION CATEGORIZATIONS
-- 
-- * * *

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
-- MED_DESCRIPTION_CAT
--
-- * * *

drop table if exists med_desc_cat_v2;
drop table if exists med_description_cat;

create table med_description_cat using delta as (
select
    cat.is_contraception,
    cat.category,
    sub.sub_category as sub_category,
    sub.description as description,
    sub.org,
    sub.data_lot,
    sub.raw_table
from 
  prj_grp_womens_health_med_desc_cat_v2.meddescriptioncatv2 sub
  left outer join prj_grp_womens_health_med_cat_v2.medcatv2 cat on sub.sub_category = cat.sub_category
)
;

-- COMMAND ----------

select * from prj_grp_womens_health_med_cat_v2.medcatv2;

-- COMMAND ----------

-- * * *
-- 
-- PROC_CAT
-- This table has both codes (from he) and descriptions
-- 
-- * * *

drop table if exists proc_desc_cat_v2;
drop table if exists proc_cat;

create table proc_cat using delta as (
  select distinct
    category,
    sub_category,
    procedure_code_description as description,
    procedure_code as code
  from 
    prj_grp_womens_health_proc_desc_cat_v2.procdescriptioncatv2
)
;

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
  select * from (
    select distinct
      rx.org,
      rx.org_patient_id,
      rx.patient_id,
      rx.code,
      rx.med_description,
      coalesce(vs_cat.category, cat.category) as category,
      coalesce(vs_cat.category, cat.sub_category) as sub_category,
      cat.is_contraception,
      rx.start_date as start_date,
      if(vs_cat.category is not null, 'code', 'description') as source,
      rx.data_lot
    from rx
      left outer join value_set vs on vs.code = rx.code
      left outer join med_value_set_cat vs_cat on vs.oid = vs_cat.oid
      left outer join med_description_cat cat on rx.med_description = cat.description
    where (rx.code is not null or rx.med_description is not null)
  )
where 
  (org != 'he') or
  (org = 'he' and category is not null)
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
  if(desc.category = 'NOT BIRTH CONTROL','NO',null) is_contraception,
  proc.encounter_date as start_date,
  if(code.category is not null, 'code', 'description') source,
  proc.data_lot
from 
  proc
  left outer join proc_cat code on proc.procedure_code = code.code
  left outer join proc_cat desc on proc.procedure_code_description = desc.description
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
  if(is_contraception = 'NO',0,1) is_contraception,
  start_date,
  source,
  data_lot
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
  sub_category,
  if(is_contraception = 'NO',0,1) is_contraception,
  start_date,
  source,
  data_lot
from patient_med_cat
);

-- COMMAND ----------

-- * * *
--
-- DISTRIBUTION OF CONTRACEPTION FOR TOTAL POPULATION
--
-- * * *

drop table if exists contraception_category_counseling;

create table contraception_category_counseling as (
select
  cat.category category,
  count(distinct inc.patient_id) total,
  count(distinct inc.patient_id) - count(distinct couns.patient_id) not_counseled,
  count(distinct couns.patient_id) counseled,
  (select count(distinct inc.patient_id)
   from include_for_contraception inc 
   join enc on inc.patient_id = enc.patient_id
   where enc.data_lot = 'LOT 2' and year(enc.encounter_date) > 2019
  ) total_included_patients,
  (select count(distinct inc.patient_id)
   from include_for_contraception inc 
   join enc on inc.patient_id = enc.patient_id and enc.data_lot = 'LOT 2' and year(enc.encounter_date) > 2019
   join counseling couns on couns.patient_id = inc.patient_id
  ) total_included_patients_with_counseling
from
  include_for_contraception inc
  join enc on inc.patient_id = enc.patient_id
  join patient_contraception_cat cat on cat.patient_id = inc.patient_id
  left outer join counseling couns on couns.patient_id = inc.patient_id
where 1=1
  and cat.data_lot = 'LOT 2'
  and year(enc.encounter_date) > 2019
  and cat.category != 'NOT BIRTH CONTROL'
group by 1
order by 1
);


-- COMMAND ----------

-- * * *
-- 
-- DIABETES STUFF
--
-- * * *

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
  
  union 
  select distinct
    org,
    org_patient_id,
    patient_id,
    null as dx_code,
    'Diabetes Diagnosis Indicated (DM Diagnosis)' as dx_description,
    'Diabetes' as category,
    data_lot,
    dmdiagnosis_date onset_date,
    null as stop_date
  from
    enc
  where
    dmdiagnosis is not null

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
-- ORG
-- 
-- * * *

drop table if exists org;

create table org using delta as select distinct org from enc;

-- COMMAND ----------

drop table if exists diabetes_screening;

create table diabetes_screening as (

  -- QUERY THE OBS TABLE
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'fglu' as test_type,
    fglu_result as test_result,
    data_lot,
    raw_table
  from
    obs
  where
    fglu_result is not null
  
  union all
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'hba1c' as test_type,
    hba1_c_result as test_result,
    data_lot,
    raw_table
  from
    obs
  where
    hba1_c_result is not null

  union all
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'ogtt' as test_type,
    ogttresult as test_result,
    data_lot,
    raw_table
  from
    obs
  where
    ogttresult is not null

  -- QUERY THE LAB TABLE
  union 
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'fglu' as test_type,
    fglu_result as test_result,
    data_lot,
    raw_table
  from
    lab
  where
    fglu_result is not null
  
  union all
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'hba1c' as test_type,
    hb_a1c_result as test_result,
    data_lot,
    raw_table
  from
    lab
  where
    hb_a1c_result is not null

  union all
  select
    org,
    patient_id,
    patient_id preg_patient_id,
    'ogtt' as test_type,
    ogttresult as test_result,
    data_lot,
    raw_table
  from
    lab
  where
    ogttresult is not null
)
;

-- COMMAND ----------

show tables in covid_bronze
;

-- COMMAND ----------

select * from prj_grp_covid_demo_race_nachc.demo_race_nachc

-- COMMAND ----------

show tables in prj_raw_covid_demo_race_nachc;

-- COMMAND ----------

select * from prj_raw_covid_demo_race_nachc.covid_nachc_demo_race_nachc_2021_03_31race_mappings_ru_csv;

-- COMMAND ----------

select * from covid.demo_race_nachc;

-- COMMAND ----------

-- * * *
-- 
-- TODO: Using race mappings from covid for now, need to restructure this
--
-- * * *
drop table if exists demo_race_nachc;
create table demo_race_nachc as select * from covid.demo_race_nachc;

-- COMMAND ----------

drop table if exists patient_race_nachc_src;
create table patient_race_nachc_src as (
  select distinct 
    demo.patient_id,
    demo.org_patient_id,
    demo.org,
    max(demo.race) race,
    max(race.race_nachc) race_nachc
  from
    demo
    left outer join demo_race_nachc race on lower(race.race) = lower(demo.race)
  group by 1,2,3
);

-- COMMAND ----------

drop table if exists patient_race_nachc;
create table patient_race_nachc as (
  select
    race.patient_id,
    race.org_patient_id,
    race.org,
    race.race,
    (case
        when lower(demo.ethnicity) in ('hispanic','hispanic or latino','hispanic/latino') then 'Latino'
        else race.race_nachc
        end
    ) race_nachc
  from
    patient_race_nachc_src race
    join demo on demo.patient_id = race.patient_id
);

-- COMMAND ----------

select count(distinct patient_id) from demo
union all 
select count(distinct patient_id) from patient_race_nachc;

-- COMMAND ----------

select distinct race, count(distinct patient_id)
from patient_race_nachc
where 1=1
  and race is not null 
  and race_nachc is null
group by 1
order by 1
;


-- COMMAND ----------

-- * * *
-- 
-- Pregnacies to be considered for pp analysis
-- pregnancies where the min_est_delivery_date is equal to the max_est_delivery_date
--
-- * * *

drop table if exists pp_pregnancy;
create table pp_pregnancy as (
  select 
    org,
    org_patient_id,
    patient_id,
    max(est_delivery_date) est_delivery_date,
    max(est_delivery_date) max_est_delivery_date,
    min(est_delivery_date) min_est_delivery_date
  from pregnancy 
    where data_lot != 'LOT 1'
    and year(est_delivery_date) > 2019
    and org in ('ac','ochin')
  group by 1,2,3
  having max_est_delivery_date = min_est_delivery_date
);


-- COMMAND ----------

drop table if exists first_pp_visit;
create table first_pp_visit as (
  select
    preg.org,
    preg.org_patient_id,
    preg.patient_id,
    preg.est_delivery_date,
    min(enc.encounter_date) fist_pp_visit,
    cast(datediff(min(enc.encounter_date), preg.est_delivery_date) as int) days_after,
    cast(datediff(min(enc.encounter_date), preg.est_delivery_date)/7 as int) weeks_after,
    cast(datediff(min(enc.encounter_date), preg.est_delivery_date)/28 as int) months_after
  from
    pp_pregnancy preg
    join enc on enc.patient_id = preg.patient_id and enc.encounter_date > preg.est_delivery_date
  group by 1,2,3,4
);
