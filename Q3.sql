
-- Q3
-- Find the 5 most popular products sold over the weekends.

WITH sold
AS (select p.PRODUCT_NAME,t.Week_no,t.DAY_OF_Week,sum(s.QUANTITY) Total,ROW_NUMBER() OVER (
          PARTITION BY t.Week_no ORDER BY sum(s.QUANTITY) DESC) top
from data_warehouse_metro.sales s join  data_warehouse_metro.time t on(t.TIME_ID=s.TIME_ID)
join data_warehouse_metro.product p on(p.PRODUCT_ID=s.PRODUCT_ID)
where DAY_OF_Week='SUNDAY' or  DAY_OF_Week='SATURDAY'
GROUP BY p.PRODUCT_NAME,t.Week_no
order by t.Week_no,sum(s.QUANTITY) desc )
SELECT *
FROM sold
WHERE top <= 5;