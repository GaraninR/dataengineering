select count(a.first_name), a.first_name, c."name" from actor a
join film_actor fa 
on fa.actor_id = a.actor_id
join film_category fc
on fc.film_id = fa.film_id
join category c
on fc.category_id = c.category_id
where c."name" = 'Children'
group by c."name", a.first_name
order by count(a.first_name) desc
fetch next 3 rows with ties;