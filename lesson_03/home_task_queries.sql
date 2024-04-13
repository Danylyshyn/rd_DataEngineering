/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT category.name,
       count(film_category.film_id) AS film_count
FROM category
         INNER JOIN film_category ON film_category.category_id = category.category_id
GROUP BY category.category_id
ORDER BY film_count DESC;





/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT count(rental_id) AS rents_cnt,
       actor.first_name,
       actor.last_name
FROM rental
         LEFT JOIN inventory ON inventory.inventory_id = rental.inventory_id
         LEFT JOIN film ON film.film_id = inventory.film_id
         LEFT JOIN film_actor ON film_actor.film_id = film.film_id
         LEFT JOIN actor ON actor.actor_id = film_actor.actor_id
GROUP BY actor.actor_id
ORDER BY rents_cnt DESC
LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT sum(amount)   AS Sales,
       category.name AS Category
FROM payment
         FULL JOIN rental ON rental.rental_id = payment.rental_id
         LEFT JOIN inventory ON inventory.inventory_id = rental.inventory_id
         LEFT JOIN film_category ON film_category.film_id = inventory.film_id
         LEFT JOIN category ON category.category_id = film_category.category_id
GROUP BY Category
ORDER BY Sales DESC
LIMIT 1;






/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT title
FROM film
         FULL JOIN (SELECT title                         AS Film_title,
                           film.film_id                  AS Film_id,
                           count(inventory.inventory_id) AS Inv
                    FROM film
                             FULL JOIN inventory ON film.film_id = inventory.film_id
                    GROUP BY film.Film_id) AS Categories ON Categories.Film_id = film.film_id
WHERE Categories.Inv = 0
ORDER BY title;




/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
       actor.first_name,
       actor.last_name,
       count(film.film_id) AS films,
       category.name
FROM film
         LEFT JOIN film_category ON film_category.film_id = film.film_id
         LEFT JOIN category ON category.category_id = film_category.category_id
         LEFT JOIN film_actor ON film_actor.film_id = film.film_id
         LEFT JOIN actor ON actor.actor_id = film_actor.actor_id
WHERE category.name = 'Children'
GROUP BY actor.actor_id, category.category_id
ORDER BY films DESC
LIMIT 3;


