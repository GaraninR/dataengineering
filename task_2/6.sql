select c.city, 
	(select count(*) from customer ctmr 
	join address a 
	on a.address_id = ctmr.address_id 
	where a.city_id = c.city_id
	and ctmr.active = 1) as count_of_active, 
	(select count(*) from customer ctmr 
	join address a 
	on a.address_id = ctmr.address_id 
	where a.city_id = c.city_id
	and ctmr.active = 0) as count_of_unactive 
from city c
order by count_of_unactive desc;