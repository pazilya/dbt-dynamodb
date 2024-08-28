{{ config({ "materialized":'table',
 "alias":'USINDSSP2020',
 "schema": 'PUBLIC'
})}}
 
WITH WRK_USINDSSP2020 AS(
SELECT SRC.SOURCE_DATA:company::varchar AS Company
,SRC.SOURCE_DATA:"company name"::varchar AS Company_Name
,SRC.SOURCE_DATA:"company ticker"::varchar AS Company_Ticker
,SRC.SOURCE_DATA:date::varchar AS Date
,SRC.SOURCE_DATA:frequency::varchar AS Frequency
,SRC.SOURCE_DATA:indicator::varchar AS Indicator
,SRC.SOURCE_DATA:"indicator name"::varchar AS Indicator_Name
,SRC.SOURCE_DATA:"indicator unit"::varchar AS Indicator_Unit
,SRC.SOURCE_DATA:scale::number AS Scale
,SRC.SOURCE_DATA:"stock exchange"::number AS Stock_Exchange
,SRC.SOURCE_DATA:"stock exchange name"::varchar AS Stock_Exchange_Name
,SRC.SOURCE_DATA:units::varchar AS Units
,SRC.SOURCE_DATA:value::number AS Value
,CURRENT_TIMESTAMP(9) AS INSERT_DTS
,CAST(CURRENT_SESSION() AS VARCHAR)  AS PROCESS_ID
,'transform_usindssp2020_load'AS PROCESS
,SRC.SOURCE_FILE_NAME
,SRC.SOURCE_FILE_ROW_NUMBER
 
FROM {{ref('copy_landing_raw_usindssp2020_load')}} SRC
 
)
 
SELECT 
Company as Company
,Company_Name as Company_Name
,Company_Ticker as Company_Ticker
,TO_DATE(TO_VARCHAR(TO_DATE(DATE, 'DD-MM-YYYY'), 'YYYY-MM-DD')) as Date
,Frequency as Frequency
,Indicator as Indicator
,Indicator_Name as Indicator_Name
,Indicator_Unit as Indicator_Unit
,Scale as Scale
,Stock_Exchange as Stock_Exchange
,Stock_Exchange_Name as Stock_Exchange_Name
,Units as Units
,Value as Value
,INSERT_DTS as INSERT_DTS
,PROCESS_ID as PROCESS_ID
,PROCESS as PROCESS
,SOURCE_FILE_NAME as SOURCE_FILE_NAME
,SOURCE_FILE_ROW_NUMBER as SOURCE_FILE_ROW_NUMBER
FROM WRK_USINDSSP2020