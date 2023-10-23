# Analytics

## Airflow

### DAG design
Refer to [Airflow's best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag) and [Astronomer's best practices](https://docs.astronomer.io/learn/dag-best-practices).
Below are additional ones.

#### Involve one final destination
This is an extension to [Astronomer's "Keep tasks atomic"](https://docs.astronomer.io/learn/dag-best-practices#keep-tasks-atomic).
Each DAG should have one destination (e.g. S3, Redshift, Email) only. 
This is to encourage a linear data flow and avoid any side effect.
An exception to this is when using S3 to stage data uploaded to Redshift, which data are loaded to both S3 and Redshift.

### DAG naming convention
If principle ["Involve one final destination"](#involve-one-final-destination) is followed, the DAG will fall under one of below,
1. External -> Internal

|Action|Naming convention|
|--------|-------|
|Import external data|`import_{data_name}_from_{source}_to_{destination}`|

2. Internal -> External

|Action|Naming convention|
|--------|-------|
|Export data to file system or database| `export_{data_name}_from_{source}_to_{destination}`|
|Send email| `send_{data_name}_to_{recipient}`|

3. Internal -> Internal 

|Action|Naming convention|
|--------|-------|
|Run process that does not move data cross-boundary| `run_{process_name}`|

#### Sources and destinations
Here are the sources and destinations we are using.

|Source|Description|
|--------|--------|
|mssql|SQL Server PROD|
|outlook_mail|[Outlook mail API](https://learn.microsoft.com/en-us/graph/outlook-mail-concept-overview)|
|redshift|Analytics team's AWS Redshift|
|s3|AWS S3|

|Destination|Description|
|--------|--------|
|redshift|Analytics team's AWS Redshift|
|s3|AWS S3|
|sftp|sFTP|
|tableau|Tableau server|