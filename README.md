# Data Engineer for AI Applications: Project 4 - Data Lake

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts the data from S3, processes them into dimensional tables using Spark on an AWS EMR cluster and loads them back into S3 in parquet format.

## How to run 
0. Create config file *dl.cfg* and insert AWS Key & Secret like:
<pre>[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''</pre>

1. run *etl.py* with 
<pre>python etl.py</pre>