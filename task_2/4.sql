select f.title from film f
left join inventory i
on i.film_id = f.film_id
where i.film_id is null