-- Databricks notebook source
-- * * *
-- 
-- 2021-01-22
-- COVID-19
-- Initial Database Tables 
-- 
-- * * *

-- COMMAND ----------

-- * * * 
--
-- INIT SESSION
-- 
-- * * *

create database if not exists covid;
use covid;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

-- * * *
--
-- BASE TABLES
-- 
-- * * *

drop table if exists admin;
refresh table prj_grp_covid_admin.admin;
create table admin using delta as select * from prj_grp_covid_admin.admin;

drop table if exists demo;
refresh table prj_grp_covid_demo.demographics;
create table demo using delta as select * from prj_grp_covid_demo.demographics;

drop table if exists dx;
refresh table prj_grp_covid_dx.diagnosis;
create table dx using delta as select * from prj_grp_covid_dx.diagnosis;

drop table if exists enc;
refresh table prj_grp_covid_enc.encounter;
create table enc using delta as select * from prj_grp_covid_enc.encounter;

drop table if exists hosp;
refresh table prj_grp_covid_hosp.hospitalization;
create table hosp using delta as select * from prj_grp_covid_hosp.hospitalization;

drop table if exists lab;
refresh table prj_grp_covid_lab.labs;
create table lab using delta as select * from prj_grp_covid_lab.labs;

drop table if exists symp;
refresh table prj_grp_covid_symp.symptoms;
create table symp using delta as select * from prj_grp_covid_symp.symptoms;

drop table if exists tobacco;
refresh table prj_grp_covid_tobacco.tobacco;
create table tobacco using delta as select * from prj_grp_covid_tobacco.tobacco;

drop table if exists vitals;
refresh table prj_grp_covid_vitals.vitals;
create table vitals using delta as select * from prj_grp_covid_vitals.vitals;

drop table if exists loinc;
refresh table prj_grp_loinc_loinc.Loinc;
create table loinc using delta as select * from prj_grp_loinc_loinc.Loinc;

