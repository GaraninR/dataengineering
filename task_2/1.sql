select COUNT(*) as COUNT_IN_CATEGORY, C."name" as CATEGORY_NAME from film f
left join film_category fc
on F.film_id = FC.film_id
left join category c
on C.category_id = FC.category_id
group by C."name"
order by COUNT(*) desc;