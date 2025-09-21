SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sijdcbsdjicbsdicjbsd.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result

-- oUR PARQUEST ILE LOCATION :
-- https://sijdcbsdjicbsdicjbsd.blob.core.windows.net/olistdata/silver/part-sducbsdjbc-311-1.c000.snappy.parquet

--Creating a view on the data, to create a view we need to create a schema first
Create schema gold  
CREATE VIEW gold.final
as 
Select * FROM
    OPENROWSET(
        BULK 'https://sijdcbsdjicbsdicjbsd.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result

Select top 100 * from gold.final

-- Creating view only for delivered items
CREATE VIEW gold.delivered_final
as 
Select * FROM
    OPENROWSET(
        BULK 'https://sijdcbsdjicbsdicjbsd.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result2
    where order_status='delivered'

Select top 100 * from gold.delivered_final 

