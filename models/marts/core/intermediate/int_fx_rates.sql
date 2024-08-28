{{ 
config(
    materialized='view', 
      tags=["hourly"]
    ) 
}}

WITH nummedup AS (
    SELECT 
        ifx.*
        ,ROW_NUMBER() OVER (PARTITION BY ifx.currency, ifx.exchange_date 
                                ORDER BY ifx.exchange_value ASC
                           ) AS rowNumsByCurrPlusDate 
    FROM {{ ref('stg_koenoma_fx_rates') }} ifx
),
dupclosevals AS (
    SELECT * FROM nummedup n1 
    WHERE (currency, exchange_date) IN 
        (SELECT currency, exchange_date FROM nummedup n2 WHERE n2.rowNumsByCurrPlusDate > 1)  
    AND n1.rowNumsByCurrPlusDate > 1
)
 
select * from {{ ref('stg_koenoma_fx_rates') }} 
 where indicator_name = 'Close' 
   and frequency = 'D' 
   and exchange_date > '2016-01-01'
   and (currency, exchange_date, exchange_value) NOT IN 
    (SELECT currency, exchange_date, exchange_value FROM dupclosevals)