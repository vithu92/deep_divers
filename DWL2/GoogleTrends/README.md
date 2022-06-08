<p>Author: Alexandra Alinka Zimmermann</p>
<p>Date: 08.06.2022</p>
<p>Team: Deep-Divers</p>
<p>Project Name: Data-Driven Restaurants. How To Use Data to Create Attractive Menus?</p>
<p>Project Part 2 (Data Warehouse and Data Lake Systems 2)</p>

<p>Data Source: Google Trends API</p>
Goal: To clean the food and diet data stored in the data lake and build a data warehouse.
<p></p>Frequency: Every Sunday</>
<ol>
<li>Fetch & Load<br/>
First, the data is extracted from the Google Trends API and loaded as 6 CSV files into an S3 Bucket on AWS. This part is the same as the project part 1. <br>
The corresponding code is saved into the Python script: Fetch_Load_S3.py
<br> <i>Please find the code in DWL1/GoogleTrends/Fetch_Load_S3</i></li>
<li>Clean and Write Data Into Data Warehouse<br/>
After that the data was fetched and saved into S3 bucket, each Sunday, the data from the most recent CSV file of each of the 6 categories is retrieved. The data is then cleaned and normalised. Finally, it is written into its corresponding data table in the data warehouse. <br>
The corresponding code is saved into the Python script: dw_clean_insert_data.py</li>
<li>dw_clean_insert_data_layer.zip<br/>
This is the Lambda layer which I used to create the Lambda function. I stored it in the S3 bucket.
</li>
</ol>

<p><b>How to run the code </b></p>
Python version for the Lambda functions: 3.7

<ol>
<li>Create a Lambda function which will run the Python script (dw_clean_insert_data.py): it cleans, normalises the data and writes it into the tables stored in the data warehouse. Use the Lambda layer <i>dw_clean_insert_data_layer.zip</i> for the Lambda function. To add the layer to the Lambda function using the zip file, store the zip file into the S3 bucket. Create a layer and select "Uploading a file from Amazon S3" and use its Object URL from the zip file. Then, When adding the Lambda layer, select the newly created Lambda layer.</li>
<li>In the Lambda function, create environment variables for the S3 bucket name, the data warehouse name, the host, the username, and the password. And make them secret. </li>
<li>Paste the code from dw_clean_insert_data.py into the Lambda function and adjust the names of the environment variables to the ones chosen.</li>
<li> Adjust the timeout to of the lambda function to 14 minutes and 59 secondes. </li>
<li>Deploy the changes and run the code.</li>
<li>Schedule the code using EventBridge for Sundays at 18h30 (16h30 UTC)

The data will be written into the tables in the PostgreSQL database, namely the data warehouse.
</ol>
