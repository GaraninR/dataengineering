select sum(p.amount) as sum_amount, c."name" as category_name from payment p
join rental r
on p.rental_id = r.rental_id
join inventory i
on r.inventory_id = i.inventory_id 
join film_category fc
on i.film_id = fc.film_id 
join category c
on c.category_id =fc.category_id
group by c."name" 
order by sum(p.amount) desc
limit 1;