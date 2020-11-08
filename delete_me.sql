
-- 
-- infertility table
--

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


