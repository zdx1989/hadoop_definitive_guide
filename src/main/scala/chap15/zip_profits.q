drop table if exists zip_profits;
create table if not exists zip_profits (
    sales_vol double,
    zip int
);

insert overwrite table zip_profits
select sum(w.price * s.qty) as sales_vol, s.zip
from sales s join widgets w
on s.widget_id = w.id
group by s.zip;