select concat(a.first_name, ' ', a.last_name) actor_name, count(r.rental_id) rental_count from rental r 
join inventory i 
on i.inventory_id  = r.inventory_id
join film f 
on i.film_id = f.film_id 
join film_actor fa 
on fa.film_id = f.film_id 
join actor a 
on a.actor_id = fa.actor_id
group by actor_name
order by count(r.rental_id) desc;