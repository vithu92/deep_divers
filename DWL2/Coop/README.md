<p>Author: Vithushan Mahendran</p>
<p>Date: 24.04.2022</p>
<p>Team: Deep-Divers</p>
<br><p>Project Name<br>Data-Driven Restaurants.<br>How To Use Data to Create Attractive Menus?
<br><br>
<p>Data Source: Coop.ch</p>
<p>Goal: With the help of web scrapping fetching all data of coop products
Frequency: Every Monday and Thursday 

Using python packages selenium and webdriver_manager, we fetch the html body for each food category of coop webpages and
filter the targeted data. This data has been saved as csv and saved to S3 Bucket.
Additionally, the latest data will be loaded to the AWS RDS Postgres SQL database
</p>
<br><br>
<p><b>Changes & additional features</b></p>
<ul>
<li>Optimised updating data into RDS Database (PostgreSQL)</li>
<li>Additionally creating a master_csv file including all products instead seperate csv for each Coop product categories</li>
<li>Functions to unify units variation within the products and compute the price per 100 unit for each product</li>
<li>Creating additional tables by joining multiple data lakes with complex computations</li>
<li>Removing brakets and other characters from strings which are transformed previously by json parsing</li>
</ul>