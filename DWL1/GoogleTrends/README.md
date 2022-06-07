<p>Author: Alexandra Alinka Zimmermann</p>
<p>Date: 24.04.2022</p>
<p>Team: Deep-Divers</p>
<p>Project Name: Data-Driven Restaurants. How To Use Data to Create Attractive Menus?</p>

<p>Data Source: Google Trends API</p>
Goal: To extract the food and diet preferences of the population in Switzerland.
<p></p><br>Frequency: Every Sunday</>
<ol>
<li>Fetch & Load<br/>
First, the data was extracted from the Google Trends API and loaded as 6 CSV files into an S3 Bucket on AWS.
The corresponding code is saved into the Python script: Fetch_Load_S3.py</li>
<li>Write Data into Tables<br>
From the CSV files stored in the S3 Bucket, the data from the most recent CSV file of each of the 6 categories was written into its corresponding PostgreSQL table in the data lake.
The corresponding code is saved into the Python script: Write_Data_Table.py</li>
<li>Validate Data in Tables<br/>
After that the data was written into the tables, a validation table was created for each of the 6 tables in the data lake. They validate that the correct number of data points were written into the tables.
The corresponding code is saved into the Python script: tables_data_validation.py</li>
</ol>

<p><b>How to run the code </b></p>
Python version for the Lambda functions: 3.7

<ol>
<li>Create lambda function and an S3 Bucket.</li>
<li>Create an encrypted environment variable containing the name of the S3 Bucket.</li>
<li>Paste the code from Fetch_Load_S3.py into the function and adjust the Lambda function name to your chosen Lambda function name and your S3 Bucket name.</li>
<li>Deploy the changes and run the code.<br/></li>


The data will be then fetched and uploaded into the S3 bucket.

<li>Create a PostgreSQL database.</li>
<li>Create another lambda function which will run the Python code to write the data into the tables.</li>
<li>Create environment variables for the S3 Bucket name, the database name, username, the password, and the database endpoint.</li>
<li>Paste the code from Write_Data_Table.py into the Lambda function and adjust the names of the environment variables to the ones you chose.</li>
<li>Deploy the changes and run the code.</li>

The data will be written into the tables in the PostgreSQL database.

<li>Create a last lambda function which will contain the Python code to validate the data that was written into the data tables</li>
<li>Create environment variables for the the database name, the username, the password, and the database endpoint.</li>
<li>Paste the code from tables_data_validation.py and adjust the names of the environment variables.</li>
<li>Deploy the changes and run the code.</li>


The data will be validated and 6 additional tables will be created.
</ol>
