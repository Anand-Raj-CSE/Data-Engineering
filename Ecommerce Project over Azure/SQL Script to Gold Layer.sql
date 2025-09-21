--CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'sdihcbsdicvbsd';
--CREATE DATABASE SCOPED CREDENTIAL dcjbd WITH IDENTITY = 'Managed Identity';

SELECT * FROM sys.database_credentials

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://brazillianecommolist.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = dcjbd
);

CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final;