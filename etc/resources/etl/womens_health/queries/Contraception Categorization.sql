-- Databricks notebook source
-- * * *
-- 
-- (2021-01-31)
-- Contraception Categorization
-- 
-- * * *

-- COMMAND ----------

-- * * *
--
-- INIT THE SESSION
--
-- * * *

use womens_health;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION by METHOD (RX or PROC)
-- 
-- * * *

select
  is_contraception `Is Contraception`,
  type `Type`,
  category `Category`,
  sub_category `Sub Category`,
  code `Code`,
  description `Description`,
  count (distinct patient_id) `Distinct Patient Count`
from
  patient_contraception_cat
where data_lot = 'LOT 2'
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
;

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION by SUB-CATEGORY
-- 
-- * * *

select
  is_contraception `Is Contraception`,
  type `Type`,
  category `Category`,
  sub_category `Sub Category`,
  count (distinct coalesce(code,'null'),coalesce(description,'null')) `Distinct Code/Name Count`,
  count (distinct patient_id) `Distinct Patient Count`
from
  patient_contraception_cat
where data_lot = 'LOT 2'
group by 1,2,3,4
order by 1,2,3,4
;

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION by CATEGORY
-- 
-- * * *

select
  is_contraception `Is Contraception`,
  type `Type`,
  category `Category`,
  count (distinct sub_category) `Distinct Sub-Category Count`,
  count (distinct coalesce(code,'null'),coalesce(description,'null')) `Distinct Code/Name Count`,
  count (distinct patient_id) `Distinct Patient Count`
from
  patient_contraception_cat
where data_lot = 'LOT 2'
group by 1,2,3
order by 1,2,3
;

-- COMMAND ----------

select
  org,
  count(distinct patient_id)
from
  patient_contraception_cat
where 1=1
  and category is null
group by 1
order by 1

-- COMMAND ----------

-- * * *
--
-- NOT CONTRACEPTION (RX)
-- 
-- * * *

select distinct
  rx.org,
  count(distinct rx.med_description)
from
  rx
  left outer join value_set val on rx.code = val.code
  left outer join med_description_cat desc on rx.med_description = desc.description
where 1=1
  and rx.data_lot = 'LOT 2'
  and desc.description is null
  and val.code is null
group by 1
order by 1

-- COMMAND ----------

select org, count(*) from rx group by 1 order by 1;

-- COMMAND ----------

select * from rx where org = 'sc' and med_description is not null;

-- COMMAND ----------

select * from rx where code is null and med_description is null;

-- COMMAND ----------

select org, count(*) from rx where code is null and med_description is null group by 1

-- COMMAND ----------

select distinct raw_table from flat;

-- COMMAND ----------

-- * * *
--
-- NOT CONTRACEPTION (RX)
-- 
-- * * *

select distinct
  rx.org `Org`,
  rx.code `Code`,
  rx.med_description `Description`,
  count(distinct rx.patient_id) `Distinct Patient Count`
from
  rx
  left outer join value_set val on rx.code = val.code
  left outer join med_description_cat desc on rx.med_description = desc.description
where 1=1
  and rx.data_lot = 'LOT 2'
  and desc.description is null
  and val.code is null
group by 1,2,3
order by 1,2,3

-- COMMAND ----------

select org, count(*) from rx where code is null and med_description is null group by 1 order by 1;

-- COMMAND ----------

select * from rx where code is null and med_description is null;
