
WITH LargestMarketSegment AS ( 
    SELECT C_MKTSEGMENT, COUNT(*) AS customer_count  
    FROM dev.customer  
    WHERE C_MKTSEGMENT IS NOT NULL  
    GROUP BY C_MKTSEGMENT  
    ORDER BY customer_count DESC  
    LIMIT 1 
) 
SELECT C.C_CUSTKEY, SUM(O.O_TOTALPRICE) AS total_spent  
FROM dev.customer C  
JOIN dev.orders O ON C.C_CUSTKEY = O.O_CUSTKEY  
JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY  
JOIN dev.region R ON N.n_regionkey = R.r_regionkey  
JOIN LargestMarketSegment L ON C.c_mktsegment = L.C_MKTSEGMENT

WHERE R.r_name != 'EUROPE' 
AND O.o_orderstatus != 'F' 
AND O.o_orderpriority like '%URGENT%'  

GROUP BY C.C_CUSTKEY 
ORDER BY total_spent DESC;

-- Query2: Query that returns the customer key and the total price a customer
-- spent in descending order, for all urgent orders that are not failed for all customers who
-- are outside Europe and belong to the largest market segment. The largest market
-- segment is the market segment with the most customers. 

-- WITH LargestMarketSegment AS ( 
--     SELECT C_MKTSEGMENT, COUNT(*) AS customer_count  
--     FROM dev.customer  
--     WHERE C_MKTSEGMENT IS NOT NULL  
--     GROUP BY C_MKTSEGMENT  
--     ORDER BY customer_count DESC  
--     LIMIT 1 
-- )  
-- SELECT C.C_CUSTKEY, SUM(O.O_TOTALPRICE) AS total_spent  
-- FROM dev.customer C  
-- JOIN dev.orders O ON C.C_CUSTKEY = O.O_CUSTKEY  
-- JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY  
-- JOIN dev.region R ON N.n_regionkey = R.r_regionkey
-- WHERE R.r_name != 'EUROPE' AND O.O_ORDERSTATUS like '%URGENT%'  
-- GROUP BY C.C_CUSTKEY  
-- ORDER BY total_spent DESC;


-- # Query 1

-- SELECT O.O_ORDERKEY, O.O_TOTALPRICE, O.O_ORDERDATE , N.N_NAME 
-- FROM dev.orders O  
-- JOIN dev.customer C ON O.O_CUSTKEY = C.C_CUSTKEY  
-- JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY  
-- WHERE N.N_NAME = 'UNITED STATES'  
-- ORDER BY O.O_ORDERDATE DESC  
-- LIMIT 10;

