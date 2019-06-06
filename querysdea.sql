SELECT payment.payment_id, 
film.title, 
staff.store_id, 
film_category.category_id, 
film_actor.actor_id, 
rental.rental_id, 
rental.inventory_id, 
amount, 
payment_date 
FROM payment 
JOIN staff ON payment.staff_id = staff.staff_id 
JOIN rental ON payment.rental_id = rental.rental_id 
JOIN inventory ON rental.inventory_id = inventory.inventory_id 
JOIN film ON inventory.film_id = film.film_id 
JOIN film_category ON film.film_id = film_category.category_id 
JOIN film_actor ON film.film_id = film_actor.film_id 
LIMIT 10;
----
SELECT payment.payment_id, 
film.title AS film_title, 
staff.store_id AS store, 
category.name AS category_name, 
actor.first_name AS actor_first_name,
actor.last_name AS actor_last_name,
amount, 
payment_date 
FROM payment 
JOIN staff ON payment.staff_id = staff.staff_id 
JOIN rental ON payment.rental_id = rental.rental_id 
JOIN inventory ON rental.inventory_id = inventory.inventory_id 
JOIN film ON inventory.film_id = film.film_id 
JOIN film_category ON film.film_id = film_category.film_id 
JOIN category ON film_category.category_id = category.category_id
JOIN film_actor ON film.film_id = film_actor.film_id 
JOIN actor ON film_actor.actor_id = actor.actor_id;
---
SELECT film.title, category.name 
FROM film_category 
JOIN film ON film_category.film_id = film.film_id 
JOIN category ON film_category.category_id = category.category_id;
---
SELECT film.title, film_category.category_id, category.name
FROM film_category 
JOIN film ON film_category.film_id = film.film_id
JOIN category ON film_category.category_id = category.category_id;


# mysql -> csv

SELECT * FROM payment_dataset
INTO OUTFILE '/Users/danielespinosa/payment.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ';'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
