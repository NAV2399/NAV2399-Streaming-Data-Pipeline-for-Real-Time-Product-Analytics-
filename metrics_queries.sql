-- Conversion Rate
SELECT
    COUNT(DISTINCT user_id) FILTER (WHERE action = 'purchase') * 1.0 /
    COUNT(DISTINCT user_id) FILTER (WHERE action = 'view_product') AS conversion_rate
FROM product_events;

-- Top Viewed Products
SELECT product, COUNT(*) AS views
FROM product_events
WHERE action = 'view_product'
GROUP BY product
ORDER BY views DESC;

-- Bounce Rate
SELECT
    user_id,
    COUNT(*) AS actions_per_user
FROM product_events
GROUP BY user_id
HAVING actions_per_user = 1;
