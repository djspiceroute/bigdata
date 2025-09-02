SELECT title, country, points, price
FROM wines
ORDER BY points DESC
LIMIT 10;
SELECT country, AVG(points) AS avg_points, COUNT(*) AS n
FROM wines
GROUP BY country
ORDER BY avg_points DESC
LIMIT 20;
SELECT title, price, points
FROM wines
WHERE price > 100
ORDER BY points DESC
LIMIT 10;
select  title,country, price, points
FROM wines
WHERE price < 100
ORDER BY points DESC
LIMIT 10;
