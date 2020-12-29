-- Databricks notebook source
create database if not exists womens_health;

-- COMMAND ----------

-- * * * 
--
-- INIT SESSION
-- 
-- * * *

-- COMMAND ----------

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

--
-- Flat Files
-- 
-- The flat_file table is a cleaned version of the prj_grp_womens_health_flat.FlatFile table. 
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

refresh table prj_grp_womens_health_flat.FlatFile;
drop table if exists womens_health.flat;

create table womens_health.flat using delta as (
select
  -- demographics stuff
  if(lower(org) = 'null', null, org) org,
  if(lower(mrn) = 'null', null, mrn) as org_patient_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(mrn) = 'null' or mrn is null, 'null', mrn)
  ) as patient_id,
  if(lower(education) = 'null', null, education) education,
  if(lower(race) = 'null', null, race) race,
  if(lower(ethnicity) = 'null', null, ethnicity) ethnicity,
  if(lower(gender_identity) = 'null', null, gender_identity) gender_identity,
  if(lower(language) = 'null', null, language) language,
  if(lower(housing_situation_uds) = 'null', null, housing_situation_uds) housing_situation_uds,
  if(lower(orientation) = 'null', null, orientation) orientation,
  if(lower(sexat_birth) = 'null', null, sexat_birth) as sex_at_birth,
  if(lower(veteran_status) = 'null', null, veteran_status) veteran_status,
  if(lower(age) = 'null', null, age) age,
  if(lower(sexual_hx_detail) = 'null', null, sexual_hx_detail) sexual_hx_detail,
  coalesce(
    to_date((sexual_hx_date), "yyyy-MM-dd"), 
    to_date((sexual_hx_date), "MM/dd/yyyy"), 
    to_date((sexual_hx_date), "yyyyMMdd")) 
      sexual_hx_date,
  if(lower(transportation_med) = 'null', null, transportation_med) transportation_med,
  if(lower(transportation_non_med) = 'null', null, transportation_non_med) transportation_non_med,
  if(lower(insurance_financial_class) = 'null', null, insurance_financial_class) insurance_financial_class,
  if(lower(insurance_primary_payer) = 'null', null, insurance_primary_payer) insurance_primary_payer,
  -- enc
  coalesce(
    to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
    to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
    to_date((most_recent_encounter_date), "yyyyMMdd")) 
      most_recent_encounter_date,
  if(lower(most_recent_encounter_location) = 'null', null, most_recent_encounter_location) most_recent_encounter_location,
  if(lower(most_recent_encounter_provider) = 'null', null, most_recent_encounter_provider) most_recent_encounter_provider,
  -- contraception
  if(lower(contraceptive_counseling_done) = 'null', null, contraceptive_counseling_done) contraceptive_counseling_done,
  coalesce(
    to_date((contraceptive_counseling_date), "yyyy-MM-dd"), 
    to_date((contraceptive_counseling_date), "MM/dd/yyyy"), 
    to_date((contraceptive_counseling_date), "yyyyMMdd"))
      contraceptive_counseling_date,
  -- rx
  if(lower(contraceptive_rx_name) = 'null', null, contraceptive_rx_name) as med_description,
  coalesce(
    to_date((contraceptive_rx_date), "yyyy-MM-dd"), 
    to_date((contraceptive_rx_date), "MM/dd/yyyy"), 
    to_date((contraceptive_rx_date), "yyyyMMdd")) 
      contraceptive_rx_date,
  if(lower(contraceptive_rx_refills) = 'null', null, contraceptive_rx_refills) contraceptive_rx_refills,
  -- birth stuff
  coalesce(
    if(lower(pregnancy_edd) = 'null', null, pregnancy_edd),
    if(lower(pregnancy_est_delivery) = 'null', null, pregnancy_est_delivery)
  ) pregnancy_est_delivery,
  if(lower(birthweight_detail) = 'null', null, birthweight_detail) birthweight_detail,
  coalesce(
    to_date((birthweight_date), "yyyy-MM-dd"), 
    to_date((birthweight_date), "MM/dd/yyyy"), 
    to_date((birthweight_date), "yyyyMMdd")) 
      birthweight_date,
  if(lower(fetal_demise_code) = 'null', null, fetal_demise_code) fetal_demise_code,
  coalesce(
    to_date((fetal_demise_date), "yyyy-MM-dd"), 
    to_date((fetal_demise_date), "MM/dd/yyyy"), 
    to_date((fetal_demise_date), "yyyyMMdd")) 
      fetal_demise_date,
  if(lower(pregnancy_termination_code) = 'null', null, pregnancy_termination_code) pregnancy_termination_code,
  coalesce(
    to_date((pregnancy_termination_date), "yyyy-MM-dd"), 
    to_date((pregnancy_termination_date), "MM/dd/yyyy"), 
    to_date((pregnancy_termination_date), "yyyyMMdd")) 
      pregnancy_termination_date,
  if(lower(spontaneous_abortion_code) = 'null', null, spontaneous_abortion_code) spontaneous_abortion_code,
  coalesce(
    to_date((spontaneous_abortion_date), "yyyy-MM-dd"), 
    to_date((spontaneous_abortion_date), "MM/dd/yyyy"), 
    to_date((spontaneous_abortion_date), "yyyyMMdd")) 
      spontaneous_abortion_date,
  -- inf
  if(lower(infecund_code) = 'null', null, infecund_code) infecund_code,
  coalesce(
    to_date((infecund_date), "yyyy-MM-dd"), 
    to_date((infecund_date), "MM/dd/yyyy"), 
    to_date((infecund_date), "yyyyMMdd")) 
      infecund_date,
  -- other stuff
  if(lower(fplrange) = 'null', null, fplrange) fplrange,
  if(lower(fplvalue) = 'null', null, fplvalue) fplvalue,
  if(lower(larcmethod_code) = 'null', null, larcmethod_code) larcmethod_code,
  coalesce(
    to_date((larcmethod_date), "yyyy-MM-dd"), 
    to_date((larcmethod_date), "MM/dd/yyyy"), 
    to_date((larcmethod_date), "yyyyMMdd")) 
      larcmethod_date,
  if(lower(pisqresponse) = 'null', null, pisqresponse) pisqresponse,
  coalesce(
    to_date((pisqdate), "yyyy-MM-dd"), 
    to_date((pisqdate), "MM/dd/yyyy"), 
    to_date((pisqdate), "yyyyMMdd")) 
      pisqdate,
  -- meta
  if(lower(data_lot) = 'null', null, data_lot) data_lot,
  if(lower(raw_table) = 'null', null, raw_table) raw_table
from
  prj_grp_womens_health_flat.FlatFile
where mrn is not null
);

-- COMMAND ----------

--
-- Demographics
-- 
-- The demo_src table is a direct import of the Demographics table (prj_grp_womens_health_demo.Demographics).  
-- 
-- The demo table is a cleaned version of the demo_src table. The following modifications were made
-- - Records with null patien_id were removed (there were only a handful of records).  
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- demo_src
refresh table prj_grp_womens_health_demo.Demographics;
refresh table flat;
drop table if exists womens_health.demo_src;
create table womens_health.demo_src using delta as select * from prj_grp_womens_health_demo.Demographics;

-- demo
drop table if exists womens_health.demo;
create table demo 
using delta 
-- partitioned by (patient_id)
as (
-- data from demographics table
select 
  if(lower (org) = 'null', null, org) org,
  if(lower (health_center_id) = 'null', null, health_center_id) health_center_id,
  if(lower (patient_id) = 'null', null, patient_id) as org_patient_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
  ) as patient_id,
  cast(age as DOUBLE) age,
  if(lower (sex) = 'null', null, sex) sex,
  if(lower (sexual_orientation) = 'null', null, sexual_orientation) sexual_orientation,
  if(lower (gender) = 'null', null, gender) gender,
  if(lower (gender_identity) = 'null', null, gender_identity) gender_identity,
  if(lower (race) = 'null', null, race) race,
  if(lower (ethnicity) = 'null', null, ethnicity) ethnicity,
  if(lower (language) = 'null', null, language) language,
  if(lower (insurance) = 'null', null, insurance) insurance,
  if(lower (access_to_care) = 'null', null, access_to_care) access_to_care,
  if(lower (health_care) = 'null', null, health_care) health_care,
  if(lower (education) = 'null', null, education) education,
  if(lower (housing_status) = 'null', null, housing_status) housing_status,
  if(lower (transportation) = 'null', null, transportation) transportation,
  if(lower (veteran_status) = 'null', null, veteran_status) veteran_status,
  if(lower (agricultural_work_status) = 'null', null, agricultural_work_status) agricultural_work_status,
  if(lower (poverty_level) = 'null', null, poverty_level) poverty_level,
  if(lower (public_housing) = 'null', null, public_housing) public_housing,
  if(lower (fpl) = 'null', null, fpl) fpl,
  if(lower (udsspec_pop) = 'null', null, udsspec_pop) udsspec_pop,
  if(lower (data_lot) = 'null', null, data_lot) data_lot,
  if(lower (raw_table) = 'null', null, raw_table) raw_table
from 
  prj_grp_womens_health_demo.Demographics src
where src.patient_id is not null
-- data from flat table
union all
select distinct
  org as org,
  most_recent_encounter_location as health_center_id,
  org_patient_id as org_patient_id,
  patient_id as patient_id,
  age as age,
  sex_at_birth as sex,
  orientation as sexual_orientation,
  null as gender,
  gender_identity as gender_identity,
  race as race,
  ethnicity as ethnicity,
  language as language,
  coalesce(insurance_primary_payer, insurance_financial_class) as insurance,
  null as access_to_care,
  null as health_care,
  education as education,
  housing_situation_uds as housing_status,
  coalesce(transportation_med, transportation_non_med) as transportation,
  null as veteran_status,
  null as agricultural_work_status,
  null as poverty_level,
  null as public_housing,
  coalesce(fplvalue, fplrange) as fpl,
  null as udsspec_pop,
  null as data_lot,
  null as raw_table
from 
  womens_health.flat flat
where flat.org_patient_id is not null
)
;



-- COMMAND ----------

-- * * *
-- 
-- ENCOUNTER STUFF
--
-- * * *

-- COMMAND ----------

--
-- Encounter 
-- 
-- The enc_src table is a direct import of the Encounter table (prj_grp_womens_health_enc.Encounter).  
-- 
-- The original encounter data has a large number of instances where there are multiple records for the same
-- org, patient_id, encounter_id (multiple records for the same encounter).  These duplicate
-- records are rolled up in the enc_dup_dates table.  The enc_dup_dates table, however, does have
-- a small number of records where there are multiple dates for the org, patient_id, and encounter_id.   
-- These records are removed in the enc table.  
--
-- The use of 'max' for the pregnanacy_intention etc. is used to indicate that at least one of the
-- records for the given org, patient_id, encounter_id had that indication marked.  Where multiple
-- dates are given for the same encounter, a max date and a min date are provided.  
--

-- set time parser policy
use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- enc_src table
refresh table prj_grp_womens_health_enc.Encounter;
drop table if exists womens_health.enc_src;
create table womens_health.enc_src using delta as select * from prj_grp_womens_health_enc.Encounter;

-- enc_dup_dates table
drop table if exists womens_health.enc_dup_dates;
create table womens_health.enc_dup_dates 
using delta 
-- partitioned by (encounter_id) 
as (
select distinct
  if(lower(org) = 'null', null, org) org,
  if(lower(patient_id) = 'null', null, patient_id) org_patient_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
  ) as patient_id,  
  if(lower(encounter_id) = 'null', null, encounter_id) org_encounter_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
  ) as encounter_id,
  if(lower(encounter_date) = 'null', null, encounter_date) encounter_date_string, 
  coalesce(
    to_date(encounter_date, "yyyy-MM-dd"), 
    to_date(encounter_date, "MM/dd/yy"), 
    to_date(encounter_date, "MM/dd/yyyy"),
    to_date(encounter_date, "yyyyMMdd")
  ) as encounter_date,
  if(lower(enc_type) = 'null', null, enc_type) enc_type,
  if(lower(health_center_id) = 'null', null, health_center_id) health_center_id,
  if(lower(raw_table) = 'null', null, org) raw_table,
  max(
    coalesce(if(lower(pregnancy_intention) = 'yes', 1, null), if(pregnancy_intention is not null, 0, null), null)
  ) pregnancy_intention_yes,
  max(
    coalesce(if(lower(pregnancy_intention) = 'no', 1, null), if(pregnancy_intention is not null, 0, null), null)
  ) pregnancy_intention_no,
  max(
    coalesce(if(lower(pregnancy_intention) = 'ok either way', 1, null), if(pregnancy_intention is not null, 0, null), null)
  ) pregnancy_intention_either,
  max(
    coalesce(if(lower(pregnancy_intention) = 'unsure', 1, null), if(pregnancy_intention is not null, 0, null), null)
  ) pregnancy_intention_unsure,
  max(pregnancy_intention) pregnancy_intention,
  max(if(lower(contraceptive_counseling) = 'contraception counseling' or lower(contraceptive_counseling) = 'yes', 1, null))contraceptive_counseling,
  max(if(lower(infertility_marker) = 'null' or infertility_marker is null, 0, 1)) infertility_marker,
  max(if(lower(pregnancy_marker) = 'null', null, pregnancy_marker)) pregnancy_marker,
  max(if(lower(pregnancy_termination_marker) = 'null', null, pregnancy_termination_marker)) pregnancy_termination_marker,
  max(if(lower(sexually_active) in ('never', 'not currently'), 0, 1)) sexually_active,
  max(coalesce(
    if(lower(dmdiagnosis) in ('yes'), 1, null), 
    if(lower(dmdiagnosis) = 'null', null, dmdiagnosis)
  )) dmdiagnosis,
  -- dmdiagnosis_date
  max(dmdiagnosis_date) max_dmdiagnosis_date_string,
  min(dmdiagnosis_date) min_dmdiagnosis_date_string,
  coalesce(
    to_date(max(dmdiagnosis_date), "yyyy-MM-dd"), 
    to_date(max(dmdiagnosis_date), "MM/dd/yy"), 
    to_date(max(dmdiagnosis_date), "MM/dd/yyyy"),
    to_date(max(dmdiagnosis_date), "yyyyMMdd")
  ) as max_dmdiagnosis_date,
  coalesce(
    to_date(min(dmdiagnosis_date), "yyyy-MM-dd"), 
    to_date(min(dmdiagnosis_date), "MM/dd/yy"), 
    to_date(min(dmdiagnosis_date), "MM/dd/yyyy"),
    to_date(min(dmdiagnosis_date), "yyyyMMdd")) as min_dmdiagnosis_date,
  -- delivery_date
  max(delivery_date) max_delivery_date_string,
  min(delivery_date) min_delivery_date_string,
  coalesce(
    to_date(max(delivery_date), "yyyy-MM-dd"), 
    to_date(max(delivery_date), "MM/dd/yy"), 
    to_date(max(delivery_date), "MM/dd/yyyy"), 
    to_date(max(delivery_date), "yyyyMMdd")
  ) as max_delivery_date,
  coalesce(
    to_date(min(delivery_date), "yyyy-MM-dd"), 
    to_date(min(delivery_date), "MM/dd/yy"), 
    to_date(min(delivery_date), "MM/dd/yyyy"), 
    to_date(min(delivery_date), "yyyyMMdd")
  ) as min_delivery_date,
  -- est_delivery_date
  max(est_delivery_date) max_est_delivery_date_string,
  min(est_delivery_date) min_est_delivery_date_string,
  coalesce(
    to_date(max(est_delivery_date), "yyyy-MM-dd"), 
    to_date(max(est_delivery_date), "MM/dd/yy"), 
    to_date(max(est_delivery_date), "MM/dd/yyyy"), 
    to_date(max(est_delivery_date), "yyyyMMdd")
  ) as max_est_delivery_date,
  coalesce(
    to_date(min(est_delivery_date), "yyyy-MM-dd"), 
    to_date(min(est_delivery_date), "MM/dd/yy"), 
    to_date(min(est_delivery_date), "MM/dd/yyyy"), 
    to_date(min(est_delivery_date), "yyyyMMdd")
  ) as min_est_delivery_date,
  concat_ws(",", collect_set(data_lot)) data_lot
from enc_src
where 1 = 1
  and patient_id is not null
  and encounter_id is not null
group by 1,2,3,4,5,6,7,8,9,10
union
select distinct
  org as org,
  org_patient_id as org_patient_id,
  patient_id as patient_id,
  most_recent_encounter_date as org_encounter_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(most_recent_encounter_date) = 'null' or most_recent_encounter_date is null, 'null', most_recent_encounter_date)
  ) as encounter_id,
  most_recent_encounter_date as encounter_date_string, 
  coalesce(
    to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
    to_date((most_recent_encounter_date), "MM/dd/yy"), 
    to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
    to_date((most_recent_encounter_date), "yyyyMMdd")) 
    as encounter_date,
  null as enc_type,
  most_recent_encounter_location as health_center_id,
  raw_table as raw_table,
  -- intentionally reversed
  if(lower(pisqresponse) = 'n', 1, null) as pregnancy_intention_yes,
  if(lower(pisqresponse) = 'y', 1, null) as pregnancy_intention_no,
  if(lower(pisqresponse) = 'either', 1, null) as pregnancy_intention_either,
  if(lower(pisqresponse) = 'unsure', 1, null) as pregnancy_intention_unsure,
  coalesce(
    if(lower(pisqresponse) = 'y', 'n', null),
    if(lower(pisqresponse) = 'n', 'y', null),
    if(lower(pisqresponse) = 'either', 'either', null),
    if(lower(pisqresponse) = 'unsure', 'unsure', null)
    ) as pregnancy_intention,
  contraceptive_counseling_done as contraceptive_counseling,
  infecund_code as infertility_marker,
  if(pregnancy_est_delivery is not null, 1, null) as pregnancy_marker,
  coalesce(pregnancy_termination_code, fetal_demise_code, spontaneous_abortion_code) as pregnancy_termination_marker,
  sexual_hx_detail as sexually_active,
  null as dmdiagnosis,
  -- dmdiagnosis_date
  null as max_dmdiagnosis_date_string,
  null as min_dmdiagnosis_date_string,
  null as max_dmdiagnosis_date,
  null as min_dmdiagnosis_date,
  -- delivery_date
  null as max_delivery_date_string,
  null as min_delivery_date_string,
  null as max_delivery_date,
  null as min_delivery_date,
  -- est_delivery_date
  pregnancy_est_delivery as max_est_delivery_date_string,
  pregnancy_est_delivery as min_est_delivery_date_string,
  pregnancy_est_delivery as max_est_delivery_date,
  pregnancy_est_delivery as min_est_delivery_date,
  data_lot as data_lot
from 
  flat
where 1 = 1
  and patient_id is not null
  and most_recent_encounter_date is not null
)
;



-- COMMAND ----------

--
-- this is used in the query below
-- it should return a small number of records
-- 


select count(*) from (
    select org, patient_id, encounter_id from (
      select 
        org, patient_id, encounter_id, count(distinct encounter_date) cnt 
      from enc_dup_dates
      group by 1,2,3
      having cnt > 1
    )
)
;

-- COMMAND ----------

-- 
-- ENC
-- The actual enc (encounter) table
-- 

drop table if exists enc;
create table enc using delta as (
  select * from enc_dup_dates
  where (org, patient_id, encounter_id) not in (
    select org, patient_id, encounter_id from (
      select 
        org, patient_id, encounter_id, count(distinct encounter_date) cnt 
      from enc_dup_dates
      group by 1,2,3
      having cnt > 1
    )
  )
);

-- COMMAND ----------

--
-- org table
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists org;

create table org using delta as (
  select distinct org from enc
);

-- COMMAND ----------

-- 
-- Diagnosis
--
-- The diag_src table is a direct import of the Diagnosis table (prj_grp_womens_health_diag.Diagnosis).  
-- The diag table cleans nulls, dates, and eliminates a handful of records with null patient_id.  
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

refresh table prj_grp_womens_health_dx.Diagnosis;
drop table if exists womens_health.diag_src;
create table womens_health.diag_src using delta as select * from prj_grp_womens_health_dx.Diagnosis;

drop table if exists womens_health.diag;
create table diag using delta as (
  select
    if(lower(org) = 'null', null, org) org, 
    if(lower(org) = 'null', null, health_center_id) health_center_id, 
    if(lower(org) = 'null', null, patient_id) org_patient_id, 
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
    ) as patient_id,  
    if(lower(org) = 'null', null, encounter_id) org_encounter_id, 
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
    ) as encounter_id,
    coalesce(to_date(encounter_date, "yyyy-MM-dd"), to_date(encounter_date, "MM/dd/yyyy"), to_date(encounter_date, "yyyyMMdd")) as encounter_date, 
    coalesce(to_date(diagnosis_date, "yyyy-MM-dd"), to_date(diagnosis_date, "MM/dd/yyyy"), to_date(diagnosis_date, "yyyyMMdd")) as diagnosis_date, 
    coalesce(to_date(onset_date, "yyyy-MM-dd"), to_date(onset_date, "MM/dd/yyyy"), to_date(onset_date, "yyyyMMdd")) as onset_date, 
    coalesce(to_date(stop_date, "yyyy-MM-dd"), to_date(stop_date, "MM/dd/yyyy"), to_date(stop_date, "yyyyMMdd")) as stop_date, 
    if(lower(org) = 'null', null, diagnosis_grouper_id) diagnosis_grouper_id, 
    if(lower(org) = 'null', null, dx_code) dx_code, 
    if(lower(org) = 'null', null, dx_description) dx_description, 
    if(lower(org) = 'null', null, diagnosis_name) diagnosis_name, 
    if(lower(org) = 'null', null, category) category, 
    if(lower (data_lot) = 'null', null, data_lot) data_lot,
    if(lower(org) = 'null', null, raw_table) raw_table
  from 
     diag_src
union all
  select
    org as org, 
    most_recent_encounter_location as health_center_id,
    org_patient_id as org_patient_id,
    patient_id as patient_id, 
    most_recent_encounter_date as org_encounter_id,
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(most_recent_encounter_date) = 'null' or most_recent_encounter_date is null, 'null', most_recent_encounter_date)
    ) as encounter_id,
    coalesce(
      to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
      to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
      to_date((most_recent_encounter_date), "yyyyMMdd")) 
      as encounter_date,
    coalesce(
      to_date((larcmethod_date), "yyyy-MM-dd"), 
      to_date((larcmethod_date), "MM/dd/yyyy"), 
      to_date((larcmethod_date), "yyyyMMdd")) 
      as diagnosis_date, 
    null as onset_date, 
    null as stop_date, 
    null as diagnosis_grouper_id, 
    larcmethod_code as dx_code, 
    null as dx_description, 
    null as diagnosis_name, 
    null as category, 
    data_lot as data_lot,
    raw_table as raw_table
  from 
    flat
);



-- COMMAND ----------

--
-- Rx
--
-- The rx_src table is a direct import of the Rx table (prj_grp_womens_health_rx.Rx).  
-- The rx table cleans nulls, dates, and eliminates a handful of records with null patient_id.  
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

refresh table prj_grp_womens_health_rx.Rx;

-- rx_src
drop table if exists womens_health.rx_src;
create table rx_src using delta as select * from prj_grp_womens_health_rx.Rx;
refresh table womens_health.rx_src;

-- rx
drop table if exists womens_health.rx;
create table womens_health.rx using delta as (
  select
    if(lower(org) = 'null', null, org) org,
    if(lower(health_center_id) = 'null', null, health_center_id) health_center_id,
    if(lower(patient_id) = 'null', null, patient_id) org_patient_id,
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
    ) as patient_id,  
    if(lower(encounter_id) = 'null', null, encounter_id) org_encounter_id,
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
    ) as encounter_id,
    if(lower(encounter_date) = 'null', null, encounter_date) encounter_date_string,
    coalesce(to_date(max(encounter_date), "yyyy-MM-dd"), to_date(max(encounter_date), "MM/dd/yyyy"), to_date(max(encounter_date), "yyyyMMdd")) as max_encounter_date,
    coalesce(to_date(min(encounter_date), "yyyy-MM-dd"), to_date(min(encounter_date), "MM/dd/yyyy"), to_date(min(encounter_date), "yyyyMMdd")) as min_encounter_date,
    if(lower(code) = 'null', null, code) code,
    if(lower(med_description) = 'null', null, med_description) med_description,
    if(lower(med_dose) = 'null', null, med_dose) med_dose,
    if(lower(quantity) = 'null', null, quantity) quantity,
    if(lower(refills) = 'null', null, refills) refills,
    if(lower(med_unit) = 'null', null, med_unit) med_unit,
    if(lower(start_date) = 'null', null, start_date) start_date,
    coalesce(to_date(max(start_date), "yyyy-MM-dd"), to_date(max(start_date), "MM/dd/yyyy"), to_date(max(start_date), "yyyyMMdd")) as max_start_date,
    coalesce(to_date(min(start_date), "yyyy-MM-dd"), to_date(min(start_date), "MM/dd/yyyy"), to_date(min(start_date), "yyyyMMdd")) as min_start_date,
    if(lower(end_date) = 'null', null, end_date) end_date,
    coalesce(to_date(max(end_date), "yyyy-MM-dd"), to_date(max(end_date), "MM/dd/yyyy"), to_date(max(end_date), "yyyyMMdd")) as max_end_date,
    coalesce(to_date(min(end_date), "yyyy-MM-dd"), to_date(min(end_date), "MM/dd/yyyy"), to_date(min(end_date), "yyyyMMdd")) as min_end_date,
    if(lower(raw_table) = 'null', null, raw_table) raw_table,
    concat_ws(",", collect_set(data_lot)) data_lot
  from
    rx_src
  where
    patient_id is not null
  group by 1,2,3,4,5,6,7,10,11,12,13,14,15,16,19,22
union all
  select
    org as org,
    most_recent_encounter_location as health_center_id,
    org_patient_id as org_patient_id,
    patient_id as patient_id,
    most_recent_encounter_date as org_encounter_id,    
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(most_recent_encounter_date) = 'null' or most_recent_encounter_date is null, 'null', most_recent_encounter_date)
    ) as encounter_id,
    most_recent_encounter_date as encounter_date_string,
    coalesce(
      to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
      to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
      to_date((most_recent_encounter_date), "yyyyMMdd")) 
      as max_encounter_date,
    coalesce(
      to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
      to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
      to_date((most_recent_encounter_date), "yyyyMMdd")) 
      as min_encounter_date,
    null as code,
    med_description as med_description,
    null as med_dose,
    null as quantity,
    contraceptive_rx_refills as refills,
    null as med_unit,
    null as start_date,
    null as max_start_date,
    null as min_start_date,
    null as end_date,
    null as max_end_date,
    null as min_end_date,
    raw_table as raw_table,
    raw_table as data_lot
  from
    flat
  where
    patient_id is not null
);
refresh table rx;


-- COMMAND ----------

--
-- Obs
--
-- The obs_src table is a direct import of the Obs table (prj_grp_womens_health_obs.Observation).  
-- The obs table cleans nulls, dates, and eliminates a handful of records with null patient_id.  
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- obs_src
refresh table prj_grp_womens_health_obs.Observation;
drop table if exists womens_health.obs_src;
create table womens_health.obs_src using delta as select * from prj_grp_womens_health_obs.Observation where dummy_id is not null and lower(dummy_id) != 'null';
refresh table womens_health.obs_src;

drop table if exists womens_health.obs;
create table womens_health.obs using delta as (
select
  if(lower(org) = 'null', null, org) org,
  if(lower(health_center_id) = 'null', null, health_center_id) health_center_id,
  if(lower(dummy_id) = 'null', null, dummy_id) org_patient_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(dummy_id) = 'null' or dummy_id is null, 'null', dummy_id)
  ) as patient_id,  
  if(lower(encounter_id) = 'null', null, encounter_id) org_encounter_id,
  concat(
    if(lower(org) = 'null' or org is null, 'null', org), 
    '|',
    if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
  ) as encounter_id,
  if(lower(obs_date) = 'null', null, obs_date) obs_date_string,
  coalesce(to_date(obs_date, "yyyy-MM-dd"), to_date(obs_date, "MM/dd/yyyy"), to_date(obs_date, "yyyyMMdd")) as obs_date,
  if(lower(delivery_date) = 'null', null, delivery_date) delivery_date_string,
  coalesce(to_date(delivery_date, "yyyy-MM-dd"), to_date(delivery_date, "MM/dd/yyyy"), to_date(delivery_date, "yyyyMMdd")) as delivery_date,
  if(lower(est_delivery_date) = 'null', null, est_delivery_date) est_delivery_date_string,
  coalesce(to_date(est_delivery_date, "yyyy-MM-dd"), to_date(est_delivery_date, "MM/dd/yyyy"), to_date(est_delivery_date, "yyyyMMdd")) as est_delivery_date,
  if(lower(bmi) = 'null', null, cast(bmi as double)) bmi,
  if(lower(condoms_provided) = 'null', null, condoms_provided) condoms_provided,
  if(lower(fglu_resut) = 'null', null, fglu_resut) fglu_result,
  if(lower(hba1_cresult) = 'null', null, hba1_cresult) hba1_c_result,
  if(lower(ogttresult) = 'null', null, ogttresult) ogttresult,
  max(coalesce(if(lower(sexually_active) = 'yes', 1, null), if(lower(sexually_active) = 'no', 0, null), null)) sexually_active,
  max(if(lower(pregnancy_intention) = 'yes', 1, 0)) pregnancy_intention_yes,
  max(if(lower(pregnancy_intention) = 'no%', 1, 0)) pregnancy_intention_no,
  max(if(lower(pregnancy_intention) = '%either%', 1, 0)) pregnancy_intention_either,
  max(if(lower(pregnancy_intention) = 'unsure', 1, 0)) pregnancy_intention_unsure,
  max(if(lower(pregnancy_termination) = 'null', null, pregnancy_termination)) pregnancy_termination,
  max(if(lower(pregnancy_termination_marker) = 'null', null, pregnancy_termination_marker)) pregnancy_termination_marker,
  max(if(lower(smoking_status) = 'null', null, smoking_status)) smoking_status,
  if(lower(org) = 'null', null, org) raw_table,
  concat_ws(",", collect_set(data_lot)) data_lot
from
  obs_src
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,26
)
;


-- COMMAND ----------

-- 
-- Pregnancy
--
-- Pregnancies are determined using three separate methods
--  - Patients with an encounter of type postpartum
--  - Patients with a pregnancy diagnosis
--  - Patients with a reported est_delivery_date
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.pregnancy;
create table womens_health.pregnancy using delta as (
  -- post partum visit type
  select distinct 
    org, 
    org_patient_id,
    patient_id, 
    data_lot,
    '1' as is_post_partum, 
    cast(null as string) is_diag, 
    cast(null as string) is_est_delivery_date, 
    cast(null as date) termination_date,
    cast(null as date) delivery_date,
    cast(null as string) termination_date_src,
    cast(null as string) delivery_date_src,
    'post_partum' as inclusion_reason
  from enc 
  where lower(enc_type) like '%postpartum%'
  union 
  -- pregnancy diagnosis
  select distinct 
    org, 
    org_patient_id,
    patient_id, 
    data_lot,
    cast(null as string) is_post_partum, 
    '1' as is_diag, 
    cast(null as string) is_est_delivery_date, 
    cast(null as date) termination_date,
    cast(null as date) delivery_date,
    cast(null as string) termination_date_src,
    cast(null as string) delivery_date_src,
    'diagnosis' as inclusion_reason
  from diag 
  where lower(category) = 'pregnancy'
  union
  -- visit with due date
  select distinct 
    enc.org, 
    enc.org_patient_id,
    enc.patient_id, 
    enc.data_lot,
    cast(null as string) is_post_partum, 
    cast(null as string) is_diag, 
    '1' as is_est_delivery_date, 
    cast(null as date) termination_date,
    cast(null as date) delivery_date,
    cast(null as string) termination_date_src,
    cast(null as string) delivery_date_src,
    'est_due_date' as inclusion_reason
  from 
    enc
    left outer join obs on enc.org = obs.org and enc.patient_id = obs.patient_id
  where coalesce(enc.max_est_delivery_date, obs.est_delivery_date) is not null
) 
;


-- COMMAND ----------

--
-- Calculations of delivery_date
-- 
-- Pregnancies are determined using three separate methods
--  - Post Partum: Patients with an encounter of type postpartum
--  - Diagnosis:   Patients with a pregnancy diagnosis
--  - Est Date:    Patients with a reported est_delivery_date
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

--
-- (1) POST PARTUM
-- The delivery_date for post partum encounter is calculated as the date of the first post partum encounter.  
-- 

MERGE INTO pregnancy preg
USING (
  select * from (
    select 
      enc.org as org,
      enc.patient_id as patient_id,
      min(encounter_date) as birth_day
    from 
      enc
      join pregnancy preg on enc.org = preg.org and enc.patient_id = preg.patient_id and preg.is_post_partum = 1
    where 
      lower(enc.enc_type) like '%postpartum%'
    group by 1, 2
  ) where birth_day is not null
) births
ON preg.org = births.org and preg.patient_id = births.patient_id
WHEN MATCHED THEN
  UPDATE SET preg.delivery_date = births.birth_day, preg.delivery_date_src = 'post_partum'
;

--
-- (2) DIAGNOSIS
-- The delivery_date for patients with a diagnosis of 'pregnancy'is calculated as 6 months after the first diagnosis
-- 

MERGE INTO pregnancy preg
USING (
  select * from (
    select distinct
      diag.org,
      diag.patient_id,
      diag.encounter_id,
      date_add(min(coalesce(diag.diagnosis_date, diag.encounter_date, enc.encounter_date, diag.onset_date)), 180) birth_day
    from 
      diag
      left outer join enc on diag.org = enc.org and diag.patient_id = enc.patient_id
    where lower(category) = 'pregnancy'
    group by 1,2,3
   ) where birth_day is not null
) births
ON preg.org = births.org and preg.patient_id = births.patient_id
WHEN MATCHED THEN
  UPDATE SET preg.delivery_date = births.birth_day, preg.delivery_date_src = 'diagnosis'
;

--
-- (3) EST DELIVERY DATE
-- The delivery_date for patients with a given est delivery date is calculated as the given est delivery date
-- 

MERGE INTO pregnancy preg
USING (
  select * from (
    select 
      enc.org,
      enc.patient_id,
      max(coalesce(enc.max_est_delivery_date, obs.est_delivery_date)) as birth_day
    from 
      enc 
      left outer join obs on enc.org = obs.org and enc.patient_id = obs.patient_id
    group by 1,2
  ) where birth_day is not null
) births 
ON preg.org = births.org and preg.patient_id = births.patient_id
WHEN MATCHED THEN
  UPDATE SET preg.delivery_date = births.birth_day, preg.delivery_date_src = 'est_date'
;




-- COMMAND ----------

-- 
-- INFERTILITY
--
-- Infertility is derived as either an 
-- - infertility_marker = 1 in the encounter table
-- - diagnosis_type like '%infertil%' in the diagnosis table
-- 
-- TODO: ADD DATES TO THIS (also add col for where the determination came from)
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.inf;

create table womens_health.inf using delta as (
select distinct 
  org, 
  org_patient_id,
  patient_id, 
  '1' as inf_marker, 
  cast(null as string) inf_diag, 
  data_lot
from enc 
where infertility_marker = 1
union all 
select distinct 
  org, 
  org_patient_id,
  patient_id, 
  cast(null as string) inf_marker, 
  '1' as inf_diag, 
  data_lot
from diag
where lower(dx_description) like '%infertil%'
)
;



-- COMMAND ----------

-- 
-- HISTORECTOMY
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.hyst;
refresh table prj_grp_womens_health_other.Other;

create table womens_health.hyst using delta as (
  select 
    org, 
    patient_id as org_patient_id,    
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
    ) as patient_id,  
    encounter_id as org_encounter_id, 
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
    ) as encounter_id,
    encounter_date, 
    contraceptive_documented2__shbcp as obs,
    data_lot, 
    raw_table
  from
    prj_grp_womens_health_other.Other
  where
    lower(contraceptive_documented2__shbcp) = 'hysterectomy'
)
;

-- COMMAND ----------

-- 
-- PROCEDURE STUFF
-- 

-- COMMAND ----------

--
-- PROC
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

refresh table prj_grp_womens_health_proc.Procedure;
drop table if exists womens_health.proc;

create table womens_health.proc using delta as (
  select 
    if(lower(org) = 'null', null, org) org,
    if(lower(health_center_id) = 'null', null, health_center_id) health_center_id,
    null as health_center_name,
    if(lower(patient_id) = 'null', null, patient_id) org_patient_id,    
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(patient_id) = 'null' or patient_id is null, 'null', patient_id)
    ) as patient_id,  
    if(lower(encounter_id) = 'null', null, encounter_id) org_encounter_id,
    concat(
      if(lower(org) = 'null' or org is null, 'null', org), 
      '|',
      if(lower(encounter_id) = 'null' or encounter_id is null, 'null', encounter_id)
    ) as encounter_id,
    coalesce(to_date((encounter_date), "yyyy-MM-dd"), to_date((encounter_date), "MM/dd/yyyy"), to_date((encounter_date), "yyyyMMdd")) as encounter_date,
    if(lower(contraceptive_device_marker) = 'null', null, contraceptive_device_marker) contraceptive_device_marker,
    if(lower(procedure_code) = 'null', null, procedure_code) procedure_code,
    if(lower(procedure_code_description) = 'null', null, procedure_code_description) procedure_code_description,
    coalesce(to_date((procedure_date), "yyyy-MM-dd"), to_date((procedure_date), "MM/dd/yyyy"), to_date((procedure_date), "yyyyMMdd")) as procedure_date,
    if(lower(data_lot) = 'null', null, data_lot) data_lot,
    if(lower(raw_table) = 'null', null, raw_table) raw_table
  from 
    prj_grp_womens_health_proc.Procedure
  union all
  select distinct
    if(lower(org) = 'null', null, org) org,
    null as health_center_id,
    most_recent_encounter_location as health_center_name,
    org_patient_id,
    patient_id,
    null as org_encounter_id,
    null as encounter_id,
    coalesce(
      to_date((most_recent_encounter_date), "yyyy-MM-dd"), 
      to_date((most_recent_encounter_date), "MM/dd/yyyy"), 
      to_date((most_recent_encounter_date), "yyyyMMdd")) as encounter_date,
    null as contraceptive_device_marker,
    if(lower(larcmethod_code) = 'null', null, larcmethod_code) as procedure_code,
    null as procedure_code_description,
    coalesce(
      to_date((larcmethod_date), "yyyy-MM-dd"), 
      to_date((larcmethod_date), "MM/dd/yyyy"), 
      to_date((larcmethod_date), "yyyyMMdd")) as procedure_date,
    if(lower(data_lot) = 'null', null, data_lot) data_lot,
    if(lower(raw_table) = 'null', null, raw_table) raw_table
  from flat
    where larcmethod_code is not null
)
;


-- COMMAND ----------

--
-- PROC CAT
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists proc_cat;
refresh table prj_grp_womens_health_proc_cat.ProcCat;

create table proc_cat using delta as (
select distinct
  trim (if(lower(category) = 'null', null, category)) category,
  trim (if(lower(sub_category) = 'null', null, sub_category)) sub_category,
  trim (if(lower(contraceptive_device_marker) = 'null', null, contraceptive_device_marker)) contraceptive_device_marker,
  trim (if(lower(procedure_code) = 'null', null, procedure_code)) procedure_code,
  trim (if(lower(procedure_code_description) = 'null', null, procedure_code_description)) procedure_code_description,
  trim (if(lower(src_org) = 'null', null, src_org)) src_org,
  trim (if(lower(org) = 'null', null, org)) org,
  trim (if(lower(data_lot) = 'null', null, data_lot)) data_lot,
  trim (if(lower(raw_table) = 'null', null, raw_table)) raw_table
from 
  prj_grp_womens_health_proc_cat.ProcCat
)
;

-- COMMAND ----------

-- 
-- EXCLUDES AND INCLUDES FOR CONTRACEPTION
-- 

-- COMMAND ----------

-- 
-- excludes
--

drop table if exists exclude_for_contraception;

create table exclude_for_contraception using delta (
  -- inf
  select distinct 'INF' as reason, org, org_patient_id, patient_id from inf
  -- hyst
  union all
  select distinct 'HYST' as reason, org, org_patient_id, patient_id from hyst
  -- pregnancy
  union all
  select distinct 'PREG' as reason, org, org_patient_id, patient_id from pregnancy
  -- preg intention
  union all
  select distinct 'PREG_INT' as reason, org, org_patient_id, patient_id from enc where pregnancy_intention_yes = 1
);



-- COMMAND ----------

--
-- includes
--

drop table if exists include_for_contraception;

create table include_for_contraception using delta (
  select distinct org, org_patient_id, patient_id from enc
  minus
  select distinct org, org_patient_id, patient_id from exclude_for_contraception
);



-- COMMAND ----------

-- 
-- VALUE SET
--

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.value_set;

create table womens_health.value_set using delta as (
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
)
;

-- COMMAND ----------

--
-- CATAGORIZATIONS: RX
--
-- The following tables are used to categorize rx records into types based on
--  - description
--  - value set code
-- 

-- COMMAND ----------

-- 
-- MED_DESCRIPTION_CAT
-- 
-- Categorizes medications by the description
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.med_description_cat;
refresh table prj_grp_womens_health_med_description_cat.MedDescriptionCat;

create table womens_health.med_description_cat using delta as 
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

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists womens_health.med_value_set_cat;
refresh table prj_grp_womens_health_med_value_set_cat.MedValueSetCat;

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
)
;

-- COMMAND ----------

-- * * *
-- 
-- PATIENT CONTRACEPTION CATEGORIES
--
-- * * *

-- COMMAND ----------

--
-- patient_med_cat
-- This is based on the above matching to code and then description.  
-- 

drop table if exists patient_med_cat;

create table patient_med_cat using delta as (
  select distinct
    rx.org,
    rx.org_patient_id,
    rx.patient_id,
    rx.code,
    rx.med_description,
    coalesce(vs_cat.category, cat.category) as category,
    rx.min_start_date as start_date
  from rx
    left outer join value_set vs on vs.code = rx.code
    left outer join med_value_set_cat vs_cat on vs.oid = vs_cat.oid
    left outer join med_description_cat cat on rx.med_description = cat.description
  where (rx.code is not null or rx.med_description is not null)
);



-- COMMAND ----------

-- 
-- patient_proc_cat
-- 

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

drop table if exists patient_proc_cat;
refresh table proc;
refresh table proc_cat;

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
  code as code,
  med_description description,
  category,
  category as sub_category,
  start_date
from patient_med_cat
);


-- COMMAND ----------

-- 
-- DIABETES Table
-- 

drop table if exists diabetes;

create table diabetes as (
  select distinct 
    org, 
    org_patient_id,
    patient_id,
    dx_code,
    dx_description,
    category,
    min(onset_date) onset_date,
    max(stop_date) stop_date
  from 
    diag
  where 
    category = 'Diabetes'
  group by 1,2,3,4,5,6
  union all
  select distinct
    org,
    org_patient_id,
    patient_id,
    null as dx_code,
    'Diabetes Diagnosis Indicated (DM Diagnosis)' as dx_description,
    'Diabetes' as category,
    min_dmdiagnosis_date onset_date,
    null as stop_date
  from
    enc
  where
    dmdiagnosis is not null
);

-- COMMAND ----------

drop table if exists data_lot;

create table data_lot as (
  select distinct data_lot from enc
);

-- COMMAND ----------

show tables in womens_health;

