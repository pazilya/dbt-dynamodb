with source as(
 
    Select * from {{source('knoema_economy_data_atlas','USINDSSP2020') }}
), 
renamed as (
select 
    Company as Company,
    Company_Name as company_name,
    Company_Ticker as company_symbol,
    Stock_Exchange as stock_exchange,
    Stock_Exchange_Name as stock_exchange_name,
    Indicator as indicator,
    Indicator_Name as indicator_name,
    Units as units,
    Scale as scale,
    Frequency as frequency,
    Date as stock_date,
    Value as stock_value,
    'Knoema.Stock History' as data_source_name
from source 
) 
 
select * from renamed

