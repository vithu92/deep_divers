<p>Author: Ezgi Köker Gökgöl</p>
<p>Date: 24.04.2022</p>
<p>Team: Deep-Divers</p>
<br><p>Project Name<br>Data-Driven Restaurants.<br>How To Use Data to Create Attractive Menus?
<br><br>
<p>Data Source: Edamam Recipe Search API</p>
<p>Goal: To extract various recipes related with search queries, transform and load them to the database
Frequency: Data fetched in every 8 hours

The code uses a list of search queries and fetch recipes (and associated details) related with each of
them from Edamam Recipe Search API. Then, transform the data and loads it to the database that was 
created on AWS RDS.  </p>


<p><b>How to run the code</b><p/>
<ol>
<li>Create a Lambda function on AWS. (Python version on Lambda function: 3.7)</li>
<li>Copy and paste the whole edamam.py file to the Lambda function</li>
<li>Deploy the changes and run the code.</li>
</ol>

