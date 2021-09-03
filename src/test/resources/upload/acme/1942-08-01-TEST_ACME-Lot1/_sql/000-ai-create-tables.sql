-- Databricks notebook source
-- * * *
-- 
-- 2021-02-07
-- ai
-- Initial Database Tables 
-- 
-- * * *

-- COMMAND ----------

-- * * * 
--
-- INIT SESSION
-- 
-- * * *

create database if not exists ai;
use ai;
set spark.sql.legacy.timeParserPolicy = LEGACY;
set spark.sql.legacy.parquet.datetimeRebaseModeInWrite = LEGACY;

-- COMMAND ----------

show databases;

-- COMMAND ----------

-- * * *
--
-- BASE TABLES
-- 
-- * * *

drop table if exists cpt;
refresh table prj_grp_ai_cpt.cpt;
create table cpt using delta as select * from prj_grp_ai_cpt.cpt;

drop table if exists demo;
refresh table prj_grp_ai_demo.demo;
create table demo using delta as select * from prj_grp_ai_demo.demo;

drop table if exists enc;
refresh table prj_grp_ai_enc.enc;
create table enc using delta as select * from prj_grp_ai_enc.enc;

drop table if exists flat;
refresh table prj_grp_ai_flat.flat;
create table flat using delta as select * from prj_grp_ai_flat.flat;

drop table if exists vacc;
refresh table prj_grp_ai_vacc.vacc;
create table vacc using delta as select * from prj_grp_ai_vacc.vacc;




