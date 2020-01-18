Project: Data Pipelines with Airflow

		A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to 	their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

		The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The 		source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the 		songs the users listen to.

		This project will create operators to perform tasks such as staging the data, filling the data warehouse, and 		running checks on the data as the final step.
        
Datasets
	There are two datasets.
		- Log data: s3://udacity-dend/log_data
		- Song data: s3://udacity-dend/song_data
        
The operators
	Stage Operator
		The stage operator load any JSON formatted files from S3 to Amazon Redshift.
        The operator creates and runs a SQL COPY statement based on the parameters provided.
        The operator's parameters specify where in S3 the file is loaded and what is the target table.
		The parameters is used to distinguish between JSON file.
        The operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
        
	Fact and Dimension Operators
		utilize the provided SQL helper class to run data transformations. 
		Most of the logic is within the SQL transformations and the operator is token as input a SQL statement and target database on which to run the query against. 
		The target table will contain the results of the transformation.
		Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load.
  
	Data Quality Operator
		The operator is the data quality operator, which is used to run checks on the data itself. 
        The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
			- For example:
            	one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.  
  
  