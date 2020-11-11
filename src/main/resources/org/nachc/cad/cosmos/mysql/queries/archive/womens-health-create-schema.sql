create table demo as select * from prj_grp_womens_health_demo.Demographics;
create table diag as select * from prj_grp_womens_health_dx.Diagnosis;
create table enc as select * from prj_grp_womens_health_enc.Encounter;
create table fert as select * from prj_grp_womens_health_fert.Fertility;
create table obs as select * from prj_grp_womens_health_obs.Observation;
create table other as select * from prj_grp_womens_health_other.Other;
create table proc as select * from prj_grp_womens_health_proc.Procedure;
create table rx as select * from prj_grp_womens_health_rx.Rx;

