[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

# ingest-python

**ingest-python** is an ingestion tool written in Python (relying only on locally installable packages such as Pandas) designed to extract data from source systems and persist it into SQL Server. It is intended for small datasets, where "small" refers to data volumes that do not require big data frameworks.

The tool supports ingesting datasets larger than available memory through **Pandas chunking**. It forms the **Extract and Load** portion of an ELT pipeline.

---

## How it Works

`ingest-python` is structured around a **Base Class** that encapsulates core SQL Server operations.

Key methods include:
- **read_params**: Reads an `entity_params` table and parses parameters into a dictionary. See [Entity Params](#entity-params) for details.
- **read_history**: Retrieves the maximum value of a defined `modified` field from a history table to support incremental loads.
- **transform_data**: Aligns the source DataFrame to the target schema.
  - Drops extra fields, adds missing fields as NULL.
  - Adds `current_record` and `ingest_datetime` fields.
- **write_data**: Inserts data into the target table. For incremental loads, previously active records are marked `current_record = False` when updated.
- **write_to_history**: Logs metadata about each ingestion into a history table.

Each supported source system has its own class inheriting from the Base Class.
These subclasses expose a single `read_data` method, which can:
- Read all records in bulk.
- Perform incremental loads.
- Use chunking for large tables.

---

## Setup

### Table Definitions

Each instance requires:
1. A **definitions** file (DDL for schemas and tables).
2. An **entity_params** file (metadata and parameters for ingestion).

All tables must include:
- `ingest_datetime`
- `current_record`

Example definition file (`definitions/adventureworks.py`):

```python
def get_ddl() -> dict:
    schema = "ods_adventureworks"
    definitions = {}
    definitions[f"{schema}_schema"] = f"CREATE SCHEMA {schema};"
    definitions[f"{schema}_history"] = f"""
        CREATE TABLE [{schema}].[history](
            [id] BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            [run_id] BIGINT NOT NULL,
            [table_name] NVARCHAR(100) NOT NULL,
            [start_time] DATETIME NOT NULL,
            [end_time] DATETIME NOT NULL,
            [time_taken] INT NOT NULL,
            [rows_processed] INT NOT NULL,
            [modifieddate] DATETIME NULL
        );
    """
    definitions[f"{schema}_entity_params"] = f"""
        CREATE TABLE [{schema}].[entity_params](
            [table_name] NVARCHAR(75) NOT NULL PRIMARY KEY,
            [entity_name] NVARCHAR(75) NOT NULL,
            [business_key] NVARCHAR(75) NOT NULL,
            [modified_field] NVARCHAR(75) NULL,
            [load_method] NVARCHAR(75) NOT NULL,
            [chunksize] INT NULL,
            [active] BIT NOT NULL
        );
    """
    definitions[f"{schema}_Department"] = f"""
        CREATE TABLE [{schema}].[Department](
            [DepartmentID] SMALLINT NOT NULL,
            [Name] VARCHAR(256) NOT NULL,
            [GroupName] VARCHAR(256) NOT NULL,
            [ModifiedDate] DATETIME NOT NULL,
            [ingest_datetime] DATETIME NOT NULL,
            [current_record] BIT NOT NULL
        );
    """
    return definitions
```
### Entity Params
Entity parameters drive ingestion behavior. Example (entity_params/adventureworks_params.py):
```python
# entity_params/adventureworks_params.py
def populate_entity_list() -> dict:
    entity_params = {
        "adventureworks": """
            INSERT INTO [ods_adventureworks].[entity_params] (
                table_name
                ,entity_name
                ,business_key
                ,modified_field
                ,load_method
                ,chunksize
                ,active
            )
            VALUES (
                'Department'
                ,'HumanResources.Department'
                ,'DepartmentID'
                ,'ModifiedDate'
                ,'incremental'
                ,NULL
                ,1
            )
        ;""",
    }
    return entity_params
```
#### Parameter notes:
- **table_name**: Target table name.
- **entity_name**: Source system entity (e.g., schema.table).
- **business_key**: Unique identifier (for incremental loads).
- **modified_field**: Incrementing/change-tracking field (for incremental loads).
- **load_method**:
  - **incremental**: Updates only changed rows.
  - **truncate**: Reloads the full table each run.
  - **chunksize**: Rows per batch (NULL = default 1M rows).
- **active**: Enables/disables ingestion for this entity.

### Adding Instances to `main.py`
Each instance must be registered in the main.py run function so that the correct class is instantiated with the appropriate configuration values.
```python
# main.py
if "adventureworks" in instances:
    cls_instances["adventureworks"] = cls_dict["DBMSClass"](
        {
            "source": cnxns["adventureworks"],
            "target": cnxns["ods"],
        },
        config["ods"]["adventureworks"],
    )
```
This ensures that when the adventureworks instance is invoked, the job uses the correct class type along with the source and target connection details from the configuration.

### Configuration File
The config.yaml file contains all connection and job parameters. Example:
```yaml
# config.yaml
parameters:
  log_path: "./"
  sql_driver: "ODBC Driver 18 for SQL Server"

mdh:
  database: "mdh"
  orchestration: "orchestration"

ods:
  type: "mssql"
  server: sql-server
  port: 1433
  uid: sa
  pwd: YourStrong@Passw0rd
  database: "ods"
  trust_cert: True

  # output schemas:
  adventureworks: "ods_adventureworks"

dbms:
  adventureworks_type: "mssql"
  adventureworks: sql-server
  adventureworks_port: 1433
  adventureworks_uid: sa
  adventureworks_pwd: YourStrong@Passw0rd
  adventureworks_database: "AdventureWorks2022"
  adventureworks_trust_cert: True
```
#### Parameter Explanations
- **parameters**: general job settings
  - **log_path**: directory for the log file.
  - **sql_driver**: installed ODBC driver for SQL Server.
- **mdh**: metadata hub settings
  - **database**: name of the metadata database (must exist in SQL Server).
  - **orchestration**: schema within the mdh database where the orchestration history table resides. Both the schema and the history table must be created, see [history table](mdhhistorytable).
- **ods**: operational data store (raw data) settings
  - **type**: target server type (assumed to be mssql).
  - **server**: SQL Server hostname.
  - **port**: SQL Server port.
  - **uid**: username.
  - **pwd**: password.
  - **database**: ODS database name.
  - **trust_cert**: whether to trust the server certificate (False recommended in production).
  - **:instance**: schema for each registered instance (e.g., adventureworks).
- **:system-type**: source system connection parameters (repeat for each type). Example for dbms:
  - **:instance_type**: server type (e.g., mssql), must be supported by cnxns.
  - **:instance**: source system hostname.
  - **:instance_port**: port of the source system.
  - **:instance_uid**: username.
  - **:instance_pwd**: password.
  - **:instance_database**: source database.
  - **:instance_trust_cert**: certificate trust flag (use False in production).

### MDH History Table
The history table tracks each run:
```sql
CREATE TABLE mdh.dbo.history (
       [run_id] [BIGINT] NOT NULL,
       ,[job] [VARCHAR](255) NOT NULL
       ,[parent_id] [BIGINT] NULL,
       ,[dttm_started] [DATETIME] NOT NULL,
       ,[dttm_finished] [DATETIME] NULL,
       ,[time_taken] [INT] NULL,
       ,[run_status] [VARCHAR](9) NOT NULL
);
```
#### Columns
- **run_id**: unique identifier for the run (also logged in files).
- **job**: name of the job (e.g., ingest).
- **parent_id**: run ID of the orchestrating job; NULL indicates an orchestrator.
- **dttm_started** / **dttm_finished**: start and finish times.
- **time_taken**: duration in seconds.
- **run_status**: "succeeded" or "failed" (failed if any entity fails).

## Usage

### Deploy
Once setup has been completed, you need to run `deploy` to setup the requisite tables in the target system, this only needs to be run once before the first run, or when additional entities have been added to an existing source. Changes to entity_params will be overwritten each time `deploy` is run. If the definition of any other existing table is amended, that table will need to be dropped before running `deploy`:
```shell
python deploy.py -i *<instance>
```
You can call as many instances as you require. Instances are named the same as the definition, so for example, to deploy definition/adventureworks.py, you'd run:
```shell
python deploy.py -i adventurworks
```

### Main
Run main.py to ingest data:
```shell
python main.py -i *<instance>
```
As with `deploy` you can call as many instances as you like. The instance is named as it's set in main, for example for:
```python
if "adventureworks" in instances:
    cls_instances["adventureworks"] = cls_dict["DBMSClass"](
        {
            "source": cnxns["adventureworks"],
            "target": cnxns["ods"],
        },
        config["ods"]["adventureworks"],
    )
```
you'd call:
```shell
python main.py -i adventurworks
```
For consistency, it's suggested you use the same name as the definition.

## After each run
- The mdh history table logs each run.
- Instance-level history tables log changes only when records are ingested.

## Error Handling
- A run may gracefully fail if an entity or instance cannot be processed.
  - Other entities continue running, and logs capture the failure.
  - This prevents one bad entity from stopping the full job.
- A catastrophic fail occurs if main.py itself raises an unhandled error.
  - In this case, the mdh history table may still list the job as “running”.
  - Manual correction may be required.

## Logging
- Logs are written to the configured log_path.
- After each run, review the logs if data is missing or unexpected.

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/philipbudden/ingest-python.svg?style=for-the-badge
[contributors-url]: https://github.com/philipbudden/ingest-python/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/philipbudden/ingest-python.svg?style=for-the-badge
[forks-url]: https://github.com/philipbudden/ingest-python/network/members
[stars-shield]: https://img.shields.io/github/stars/philipbudden/ingest-python.svg?style=for-the-badge
[stars-url]: https://github.com/philipbudden/ingest-python/stargazers
[issues-shield]: https://img.shields.io/github/issues/philipbudden/ingest-python.svg?style=for-the-badge
[issues-url]: https://github.com/philipbudden/ingest-python/issues
[license-shield]: https://img.shields.io/github/license/philipbudden/ingest-python.svg?style=for-the-badge
[license-url]: https://github.com/philipbudden/ingest-python/blob/main/LICENSE
