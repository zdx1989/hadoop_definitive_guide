select year, max(temperature)
from records
where temperature != 9999
group by year;