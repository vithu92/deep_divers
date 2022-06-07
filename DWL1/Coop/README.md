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


<p><b>How to run the code</b><p/>
* python
  ```sh
  python3 -m venv venv
  ```
