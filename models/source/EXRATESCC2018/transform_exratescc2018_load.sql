{{ config({ "materialized":'table',
 "alias":'EXRATESCC2018',
 "schema": 'PUBLIC'
})}}
 
WITH WRK_EXRATESCC2018 AS(
SELECT SRC.SOURCE_DATA:currency::varchar AS Currency
,SRC.SOURCE_DATA:"currency description"::varchar AS Currency_Description
,SRC.SOURCE_DATA:"currency exchange"::varchar AS Currency_Exchange
,SRC.SOURCE_DATA:"currency name"::varchar AS Currency_Name
,SRC.SOURCE_DATA:"currency pairid"::number AS Currency_PairId
,SRC.SOURCE_DATA:"currency unit"::varchar AS Currency_Unit
,SRC.SOURCE_DATA:"currency units"::varchar AS Currency_Units
,SRC.SOURCE_DATA:date::varchar AS Date
,SRC.SOURCE_DATA:frequency::varchar AS Frequency
,SRC.SOURCE_DATA:indicator::varchar AS Indicator
,SRC.SOURCE_DATA:"indicator name"::varchar AS Indicator_Name
,SRC.SOURCE_DATA:"indicator unit"::varchar AS Indicator_Unit
,SRC.SOURCE_DATA:scale::number AS Scale
,SRC.SOURCE_DATA:value::number AS Value
,CURRENT_TIMESTAMP(9) AS INSERT_DTS
,CAST(CURRENT_SESSION() AS VARCHAR)  AS PROCESS_ID
,'transform_exratescc2018_load'AS PROCESS
,SRC.SOURCE_FILE_NAME as SOURCE_FILE_NAME
,SRC.SOURCE_FILE_ROW_NUMBER as SOURCE_FILE_ROW_NUMBER
 
FROM {{ref('copy_landing_raw_exratescc2018_load')}} SRC
 
)
 
SELECT 
Currency as Currency
,Currency_Description as Currency_Description
,Currency_Exchange as Currency_Exchange
,Currency_Name as Currency_Name
,Currency_PairId as Currency_PairId
,Currency_Unit as Currency_Unit
,Currency_Units as Currency_Units
,TO_DATE(TO_VARCHAR(TO_DATE(DATE, 'DD-MM-YYYY'), 'YYYY-MM-DD')) as Date
,Frequency as Frequency
,Indicator as Indicator
,Indicator_Name as Indicator_Name
,Indicator_Unit as Indicator_Unit
,Scale as Scale
,Value as Value
,INSERT_DTS as INSERT_DTS
,PROCESS_ID as PROCESS_ID
,PROCESS as PROCESS
,SOURCE_FILE_NAME as SOURCE_FILE_NAME
,SOURCE_FILE_ROW_NUMBER as SOURCE_FILE_ROW_NUMBER
 
FROM WRK_EXRATESCC2018