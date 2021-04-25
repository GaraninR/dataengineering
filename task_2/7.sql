with sum_all as (
	select sum((EXTRACT(EPOCH FROM r.return_date) - EXTRACT(EPOCH FROM r.rental_date))/3600) sum_hours, ct.city city, cat."name" category from rental r
	join customer c
	on r.customer_id  = c.customer_id
	join address a
	on a.address_id  = c.address_id 
	join city ct
	on a.city_id  = ct.city_id 
	join inventory i 
	on r.inventory_id = i.inventory_id 
	join film f
	on f.film_id = i.film_id
	join film_category fc 
	on fc.category_id = f.film_id 
	join category cat
	on cat.category_id = fc.category_id
	where upper(ct.city) like 'A%' or ct.city like '%-%'
	group by ct.city, cat."name" 
) 
select coalesce(sum_hours, 0) sum_fin, city, category from sum_all
order by sum_fin desc
limit 1;