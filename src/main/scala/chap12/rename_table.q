alter table target_new rename to target_old;

alter table target_old add columns (c3 string);

select * from target_old;