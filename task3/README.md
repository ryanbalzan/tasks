# Task 3 - SQL Coding

## SQL Query to Calculate Daily Transactions for April 2024

Q1: Write an SQL query to calculate for each day of April 2024:
- The daily Total Deposits amount
- The daily count of Deposit transactions
- The daily number of Unique depositors
- The daily Average Deposit amount (daily Deposits amount / daily deposits count)

### SQL Query:
```sql
SELECT CAST(transaction_date as DATE) transaction_date, 
    SUM(transaction_amount_EUR) AS total_deposit_amount,
    COUNT(transaction_type) AS number_of_deposits,
    COUNT(distinct player_id) AS unique_depositors,
    AVG(transaction_amount_EUR) AS average_deposit_amount
FROM payments
WHERE CAST(transaction_date as DATE) >= '2024-04-01' AND CAST(transaction_date as DATE) < '2024-05-01'
```



Q2: Write an SQL query to calculate for each day of April 2024
- The Total Deposits amount since the beginning of the month (i.e. from 01/04/2024 00:00:00)
- the number of Unique depositors since the beginning of the month (i.e. from 01/04/2024 00:00:00)

```sql
Declare startdate DATE DEFAULT '2024-04-01';
Declare enddate DATE DEFAULT '2024-04-30';  -- inclusive

SELECT 
    DATE(transaction_date) AS transaction_date,
    SUM(daily_total_deposits) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_total_deposits,
    COUNT(DISTINCT player_id) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_unique_depositors,
FROM daily_stats
WHERE DATE(transaction_date) >= startdate AND DATE(transaction_date) <= enddate
GROUP BY DATE(transaction_date)
ORDER BY DATE(transaction_date);
```

Given Big Query does not have COUNT(DISTINCT) OVER WINDOW
```sql
Declare startdate DATE DEFAULT '2024-04-01';
Declare enddate DATE DEFAULT '2024-04-30';  -- inclusive

SELECT ds.transaction_date, 
    sum(transaction_amount_EUR) AS total_deposit_amount,
    count(distinct player_id) unique_customers
FROM payments p
INNER JOIN (
    SELECT transaction_date
    FROM UNNEST(GENERATE_DATE_ARRAY(startdate, enddate)) AS transaction_date
 ) ds
ON DATE_TRUNC(p.transaction_date, MONTH) = DATE_TRUNC(ds.transaction_date, MONTH)  
WHERE p.transaction_date <= ds.transaction_date
GROUP BY 1
ORDER BY ds.transaction_date
```

