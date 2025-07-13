## Purpose:
## This script initializes the data warehouse environment following the medallion architecture pattern.
## It drops any existing datawarehouse database and recreates it. It then defines three schema layers:

###bronze: for raw ingested data

###silver: for cleaned and transformed data

###gold: for aggregated and reporting-ready data

⚠ Note: In MySQL, CREATE SCHEMA is equivalent to CREATE DATABASE, so the bronze, silver, and gold schemas are actually separate databases, not nested under datawarehouse.

DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

USE datawarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
