-- Databricks notebook source
-- * * * 
-- 
-- (2021-01-31)
-- 001-CONTRACEPTION-QUERIES
-- THIS IS THE UPDATED QUERIES FOR CONTRACEPTION
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
-- PATIENTS
-- 
-- * * *

-- COMMAND ----------

select org, format_number(count(distinct patient_id),0) distinct_patient_count 
from enc 
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1
order by 1;

-- COMMAND ----------

select 
  org, 
  data_lot,
  raw_table as src_file,
  format_number(count(distinct patient_id),0) distinct_patient_count,
  format_number(count(*),0) total_enc_records 
from enc 
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'group by 1,2,3
order by 1,2,3;

-- COMMAND ----------

select 
  org, 
  data_lot `Data Lot`,
  count(distinct patient_id) `Distinct Patient Count`
from enc 
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2;

-- COMMAND ----------

-- * * *
--
-- ENCOUNTERS
-- 
-- * * *

-- COMMAND ----------

--
-- encounters by month and org (chart)
--

select
  concat(year(encounter_date), 
    '-', 
    format_number(month(encounter_date), '00')
  ) `Month`,
  org.org `Org`,
  count(distinct encounter_id) `Distinct Encounters`
from
  enc
  right outer join org on enc.org = org.org
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2 

-- COMMAND ----------

--
-- encounters by month and org (chart)
--

select
  concat(year(encounter_date), 
    '-', 
    format_number(month(encounter_date), '00')
  ) `Month`,
  org.org `Org`,
  count(distinct encounter_id) `Distinct Encounters`
from
  enc
  right outer join org on enc.org = org.org
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2 

-- COMMAND ----------

-- * * *
-- 
-- PREGNANCIES BY ORG (WITH PREGNANCY RATES)
-- 
-- * * *

-- * * *
-- 
-- Total pregnancies (derived)
-- 
-- * * *

select 
  enc.org `Org`,
  format_number(count(distinct preg.patient_id),0) `Distinct Patients With Pregnancy (Derived)`,
  format_number(count(distinct enc.patient_id),0) `Distinct Total Patients`,
  format_number(count(distinct preg.patient_id)/count(distinct enc.patient_id)*100,2) `Rate`
from 
  enc
  left outer join pregnancy preg on enc.patient_id = preg.patient_id and year(preg.est_delivery_date) > 2019
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1
order by 1
;

-- COMMAND ----------

-- * * *
-- 
-- PREGNANCY INCLUSIONS (TABLE)
-- 
-- * * *

select 
  org `Org`,
  reason `Reason`,
  format_number(count(distinct patient_id),0) `Distinct Patients`
from (
  select
    preg.org,
    preg.patient_id,
    concat_ws(",", collect_set(distinct inclusion_reason)) reason
  from
    enc
    join pregnancy preg on enc.patient_id = preg.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  group by 1,2
)
group by 1,2
order by count(distinct patient_id) desc,2,1
;

-- COMMAND ----------

-- * * *
-- 
-- PREGNANCY INCLUSIONS (CHART)
-- 
-- * * *

select 
  org `Org`,
  reason `Reason`,
  count(distinct patient_id) `Distinct Patients`
from (
  select
    preg.org,
    preg.patient_id,
    concat_ws(",", collect_set(distinct inclusion_reason)) reason
  from
    enc join pregnancy preg on enc.patient_id = preg.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  group by 1,2
)
group by 1,2
order by 3 desc,2,1
;

-- COMMAND ----------

-- * * *
-- 
-- INFERTILITY
--
-- * * *

-- COMMAND ----------

select
  inf.org,
  inf.inclusion_reason,
  count(distinct inf.patient_id) distinct_inf_count,
  tot.total_patients,
  format_number((count(distinct inf.patient_id) / tot.total_patients * 100),2) inf_rate
from
  enc join
  infertility inf on enc.patient_id = inf.patient_id
  join (
    select org, count(distinct patient_id) total_patients from enc group by 1
  ) tot on inf.org = tot.org
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2,tot.total_patients
order by 1,2
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION EXCLUSION GROUP
-- 
-- * * *

select
  org `Org`,
  inclusion_reason `Inclusion Reason`,
  count(distinct patient_id) `Distinct Patients`
from (
  select
    ex.org,
    ex.patient_id,
    concat_ws(",", collect_set(distinct inclusion_reason)) inclusion_reason
  from
    enc
    join exclude_for_contraception ex on enc.patient_id = ex.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  group by 1,2
)
group by 1,2
order by 3 desc,2,1
;


-- COMMAND ----------

-- * * *
--
-- PREGNANCY INTENTION
--
-- * * *
refresh table pregnancy_intention;
select
  int.org,
  int.pregnancy_intention_marker,
  count(distinct int.patient_id)
from
  enc join pregnancy_intention int on enc.patient_id = int.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
group by 1,2
order by 1,2
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION EXCLUSION GROUP (cumulative)
-- 
-- * * *

select
  org `Org`,
  inclusion_reason `Inclusion Reason`,
  count(distinct patient_id) `Distinct Patients`
from (
  select
    ex.org,
    ex.patient_id,
    concat_ws(",", collect_set(distinct ex.inclusion_reason)) inclusion_reason
  from
    enc join exclude_for_contraception ex on enc.patient_id = ex.patient_id
    where 1=1
      and year(encounter_date) > 2019
      and enc.data_lot = 'LOT 2'
  group by 1,2
)
group by 1,2
order by 3 desc,2,1
;



-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION EXCLUSION GROUP (overlapping)
-- 
-- * * *

select
  ex.org `Org`,
  ex.inclusion_reason `Inclusion Reason`,
  count(distinct ex.patient_id) `Distinct Patients`
from 
  enc join exclude_for_contraception ex on enc.patient_id = ex.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 3 desc,2,1
;

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION EXCLUSION GROUP (overlapping)
-- 
-- * * *

select
  ex.org `Org`,
  ex.inclusion_reason `Inclusion Reason`,
  count(distinct ex.patient_id) `Distinct Patients`
from 
  enc join exclude_for_contraception ex on enc.patient_id = ex.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 3 desc,2,1
;

-- COMMAND ----------

-- * * *
-- 
-- EXCLUDES BY REASON (TABLE)
-- 
-- * * *

select
  inclusion_reason `Reason`,
  format_number(count(distinct patient_id),0) `Distinct Patients`
from (
  select 
    ex.patient_id,
    concat_ws(",", collect_set(distinct ex.inclusion_reason)) inclusion_reason
  from 
    enc join exclude_for_contraception ex on enc.patient_id = ex.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  group by 1
)
group by 1
order by count(distinct patient_id) desc,2
;

-- COMMAND ----------

-- * * *
-- 
-- INCLUDES, EXCLUDES, TOTAL
-- 
-- * * *

select '1.) Total' as `Group`, org `Org`, count(distinct patient_id) `Distinct Patients` from enc 
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2

union all
select '2.) Include' as `Group`, enc.org `Org`, count(distinct inc.patient_id) `Distinct Patients` 
from enc join include_for_contraception inc on enc.patient_id = inc.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2

union all
select '3.) Exclude' as `Group`, org `Org`, count(distinct patient_id) `Distinct Patients` from (
  select enc.org, enc.patient_id from enc
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  minus
  select enc.org, inc.patient_id 
  from enc join include_for_contraception inc on enc.patient_id = inc.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'

) group by 1,2

order by 1,2

-- COMMAND ----------

select
  org,
  enc.contraceptive_counseling,
  count(distinct enc.patient_id)
from 
  enc 
  join include_for_contraception inc on enc.patient_id = inc.patient_id
where 1=1
  and year(enc.encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
  and contraceptive_counseling is not null
group by 1,2

-- COMMAND ----------

select
  contraceptive_counseling,
  count(*)
from
  enc
where org = 'he'
group by 1


-- COMMAND ----------

select
  org,
  if(enc.contraceptive_counseling is null or enc.contraceptive_counseling = 0, null, 'y') contraceptive_counseling,
  count(distinct enc.patient_id)
from 
  enc 
  join include_for_contraception inc on enc.patient_id = inc.patient_id
where 1=1
  and year(enc.encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
  and contraceptive_counseling is not null
group by 1,2

-- COMMAND ----------

-- * * *
-- 
-- INCLUDES, EXCLUDES, TOTAL
-- 
-- * * *

select '1.) Total' as `Group`, org `Org`, count(distinct patient_id) `Distinct Patients` from enc 
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
union all
select '2.) Include' as `Group`, enc.org `Org`, count(distinct inc.patient_id) `Distinct Patients` 
from enc join include_for_contraception inc on enc.patient_id = inc.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
union all
select '3.) Exclude' as `Group`, org `Org`, count(distinct patient_id) `Distinct Patients` from (
  select enc.org, enc.patient_id from enc
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
  minus
  select enc.org, inc.patient_id 
  from enc join include_for_contraception inc on enc.patient_id = inc.patient_id
  where 1=1
    and year(encounter_date) > 2019
    and enc.data_lot = 'LOT 2'
) group by 1,2
order by 1,2

-- COMMAND ----------

-- * * *
-- 
-- Total contraception (INCLUDES ONLY -- TABLE) 
-- By ORG
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where 1=1
  and year(encounter_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1,2
order by 2,3 desc,2,1

-- COMMAND ----------

select 
  enc.org,
  count(distinct enc.patient_id)
from 
  enc
--  join patient_contraception_cat cat on enc.patient_id = cat.patient_id
  join pregnancy preg on enc.patient_id = preg.patient_id 
  join include_for_contraception inc on enc.patient_id = inc.patient_id
where 1=1 
--  and enc.org = 'ochin'
  and year(enc.encounter_date) > 2019
  -- and year(preg.est_delivery_date) > 2019
  and enc.data_lot = 'LOT 2'
group by 1

-- COMMAND ----------

-- * * *
-- 
-- LOT 2
-- Total contraception (INCLUDES ONLY -- CHART) 
-- By ORG
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
  /*left outer*/ join pregnancy preg on enc.patient_id = preg.patient_id /*and year(preg.est_delivery_date) > 2019*/
where 1=1
  and (is_contraception = 1 or category = 'EC')
  and enc.data_lot = 'LOT 2'
  and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1


-- COMMAND ----------

-- * * *
-- 
-- LOT 2
-- Total contraception (INCLUDES ONLY -- TABLE) 
-- By ORG
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`,
  cat.org as `Org`,
  count(distinct cat.patient_id) as `Distinct Patient Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where 1=1
  and (is_contraception = 1 or category = 'EC')
  and enc.data_lot = 'LOT 2'
  and year(encounter_date) > 2019
group by 1,2
order by 3 desc, 2,1

-- COMMAND ----------

-- * * *
-- 
-- LOT 2
-- Total contraception (INCLUDES ONLY -- TABLE) 
-- Totals
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  count(distinct cat.patient_id) as `Distinct Patient Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where 1=1
  and (is_contraception = 1 or category = 'EC')
  and enc.data_lot = 'LOT 2'
  and year(encounter_date) > 2019
group by 1
order by 1

-- COMMAND ----------

-- * * *
-- 
-- LOT 2
-- CATEGORIES AND SUB CATEGORIES
-- Total contraception (INCLUDES ONLY -- TABLE) 
-- By ORG
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  coalesce(cat.sub_category, '(Cat. Not Found)') as `Sub Category`, 
  count(distinct cat.patient_id) as `Distinct Patient Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where 1=1
  and (is_contraception = 1 or category = 'EC')
  and enc.data_lot = 'LOT 2'
  and year(encounter_date) > 2019
group by 1,2
order by 1,2

-- COMMAND ----------

-- * * *
-- 
-- LOT 2
-- SUBCATEGORIES Total contraception (INCLUDES ONLY -- CHART) 
-- By ORG
-- 
-- * * *

select
  coalesce(cat.sub_category, '(Cat. Not Found)') as `Sub Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where 1=1
  and is_contraception = 1
  and enc.data_lot = 'LOT 2'
  and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

-- * * *
-- 
-- CONTRACEPTION DISTRIBUTIONS FOR EACH INDIVIDUAL ORG
-- 
-- * * *


-- COMMAND ----------

-- * * *
-- 
-- AC
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
AND ENC.ORG = 'ac'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

-- * * *
-- 
-- DENVER
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
AND ENC.ORG = 'denver'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

-- * * *
-- 
-- HE
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
AND ENC.ORG = 'he'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

-- * * *
-- 
-- OCHIN
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
AND ENC.ORG = 'ochin'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

-- * * *
-- 
-- SC
-- 
-- * * *

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category`, 
  enc.org as `Org`,
  count(distinct cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
AND ENC.ORG = 'sc'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,2,1

-- COMMAND ----------

select
  org,
  contraceptive_counseling,
  count(distinct patient_id)
from enc
group by 1,2
order by 1,2  
;

-- COMMAND ----------

-- 
-- AC
-- CHART
-- Total contraception (INCLUDES ONLY) 
-- By COUNSELING
-- 

select
  coalesce(cat.category, '(Cat. Not Found)') as `Category (ALL PARTNERS)`, 
  coalesce(
    if(enc.contraceptive_counseling is null, 'Not Recorded', null), 
    'Yes'
  ) as `Counseling`,
  count(distinct cat.org, cat.patient_id) as `Count`
from
  patient_contraception_cat cat
  left outer join enc on cat.org = enc.org and cat.patient_id = enc.patient_id
  join include_for_contraception inc on cat.org = enc.org and cat.patient_id = inc.patient_id
where enc.data_lot = 'LOT 2'
and year(encounter_date) > 2019
group by 1,2
order by 3 desc,1,2

-- COMMAND ----------

-- * * *
--
-- CONTRACEPTION COUNSELING (INCLUDING NO CONTRACEPTION)
--
-- * * *

select 
  coalesce(cat.category, '0.) No Contraception Rx') Category,
  if(enc.contraceptive_counseling is null, 'Not Recorded', 'Yes') Counseling,
  count(distinct inc.patient_id) `Distinct Patients`
from
  include_for_contraception inc
  left outer join enc on inc.patient_id = enc.patient_id
  left outer join patient_contraception_cat cat on inc.patient_id = cat.patient_id 
where enc.data_lot = 'LOT 2'
and year(encounter_date) > 2019
group by 1,2
order by 1,2
;
