/*
========================================================================
DDL Script to create Schemas
========================================================================
Script purpose:
  This script cheks if schemas do not already exists and if they do not creates them in the designated database.
========================================================================
Usage:
Run to create the schemas
*/


IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
	EXEC('CREATE SCHEMA bronze');
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
	EXEC('CREATE SCHEMA silver');
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
	EXEC('CREATE SCHEMA gold');
END;
