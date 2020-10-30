
use womens_health;
-- create table demo as select * from prj_grp_womens_health_demo.Demographics;
-- create table diag as select * from prj_grp_womens_health_dx.Diagnosis;
drop table if exists enc;
drop table if exists enc_detail;
-- refresh table prj_grp_womens_health_enc.Encounter;
create table enc_detail as select * from prj_grp_womens_health_enc.Encounter;
-- create table fert as select * from prj_grp_womens_health_fert.Fertility;
-- create table obs as select * from prj_grp_womens_health_obs.Observation;
-- create table other as select * from prj_grp_womens_health_other.Other;
-- create table proc as select * from prj_grp_womens_health_proc.Procedure;
-- create table rx as select * from prj_grp_womens_health_rx.Rx;



-- 
-- WOMENS HEALTH ENCOUNTER TABLE
--


use womens_health;
drop table if exists enc;
create table enc as (
select distinct
  patient_id,
  encounter_id,
  encounter_date, 
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
  max(dmdiagnosis_date) max_dmdiagnosis_date,
  min(dmdiagnosis_date) min_dmdiagnosis_date,
  max(delivery_date) max_delivery_date,
  min(delivery_date) min_delivery_date,
  max(est_delivery_date) max_est_delivery_date,
  min(est_delivery_date) min_est_delivery_date
from enc_detail
where 1 = 1
  and patient_id is not null
  and encounter_id is not null
group by 1,2,3,4,5,6,7
)
;
