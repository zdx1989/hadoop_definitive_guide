insert overwrite table bucketed_users
select * from users;