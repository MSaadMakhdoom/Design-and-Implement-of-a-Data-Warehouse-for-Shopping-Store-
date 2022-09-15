-- Q1

-- Present total sales of all products supplied by each supplier with respect to quarter and
-- month

select l.SUPPLIER_NAME,t.QUARTER,t.MONTH,sum(s.TOTAL_SALE)
from data_warehouse_metro.sales s join data_warehouse_metro.supplier l
on(l.SUPPLIER_ID=s.SUPPLIER_ID)
join data_warehouse_metro.time t
on(t.TIME_ID=s.TIME_ID)
GROUP BY l.SUPPLIER_NAME,t.QUARTER,t.MONTH WITH ROLLUP;


-- Q2
-- Present total sales of each product sold by each store. The output should be organised
-- store wise and then product wise under each store.

select Store.STORE_ID,Store.STORE_NAME,Product.PRODUCT_NAME,sum(Sale.TOTAL_SALE)
from data_warehouse_metro.sales Sale join data_warehouse_metro.store Store on(Store.STORE_ID=Sale.STORE_ID)
join data_warehouse_metro.product Product on(Product.PRODUCT_ID=Sale.PRODUCT_ID)
GROUP BY Store.STORE_NAME,Product.PRODUCT_NAME  WITH ROLLUP;

--  Q4
-- Present the quarterly sales of each product for year 2016 using drill down query concept.
-- Note: each quarter sale must be a column.

select p.PRODUCT_NAME,
CASE t.QUARTER WHEN 1 THEN sum(s.TOTAL_SALE) ELSE NULL END as 'Quater_1',
CASE t.QUARTER WHEN 2 THEN sum(s.TOTAL_SALE) ELSE NULL END as 'Quater_2',
CASE t.QUARTER WHEN 3 THEN sum(s.TOTAL_SALE) ELSE NULL END as 'Quater_3',
CASE t.QUARTER WHEN 4 THEN sum(s.TOTAL_SALE) ELSE NULL END as 'Quater_4'
from data_warehouse_metro.sales s join data_warehouse_metro.time t on(t.TIME_ID=s.TIME_ID)
join data_warehouse_metro.product p on(p.PRODUCT_ID=s.PRODUCT_ID)
GROUP BY p.PRODUCT_NAME,t.QUARTER 
order by p.PRODUCT_NAME,t.QUARTER;


-- Q5

-- Extract total sales of each product for the first and second half of year 2016 along with its
-- total yearly sales.

select p.PRODUCT_NAME,t.Mid,sum(s.TOTAL_SALE) Total
from data_warehouse_metro.sales s join data_warehouse_metro.time t
on(t.TIME_ID=s.TIME_ID)
join data_warehouse_metro.product p
on(p.PRODUCT_ID=s.PRODUCT_ID)
GROUP BY p.PRODUCT_NAME,t.Mid with rollup;

--  Q6

select count(distinct t.CUSTOMER_ID, t.STORE_ID, t.PRODUCT_ID, m.SUPPLIER_ID, t.T_DATE) AS "Anomly"
from metro_db.transactions t join metro_db.masterdata m
on t.PRODUCT_ID = m.PRODUCT_ID;

select *
from metro_db.masterdata 
where PRODUCT_NAME ="Tomatoes";


-- Q7
DROP view IF EXISTS metro_dw.STOREANALYSIS;

CREATE VIEW STOREANALYSIS AS
SELECT s.store_id STORE_ID, p.product_id PROD_ID, SUM(l.total_sale) STORE_TOTAL
from data_warehouse_metro.store s, data_warehouse_metro.sales l,data_warehouse_metro.product p 
where s.store_id = l.store_id and l.product_id = p.product_id
GROUP by s.store_name, p.product_name
ORDER by store_name, product_name;

select * from data_warehouse_metro.STOREANALYSIS ;




























