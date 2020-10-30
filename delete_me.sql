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