<div id="top"></div>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://images.squarespace-cdn.com/content/v1/56a2785c69a91af45e06a188/1581615748179-ZXXCWKV497XS67HBIKTI/Restaurant-BigData-Sales.png?format=1500w">
    <img src="Logo.png" alt="Logo" width="400">
  </a>

<h3 align="center">Data-Driven Restaurants</h3>

  <p align="center">
    How To Use Data to Create Attractive Menus?
    <br />
    <a href="https://github.com/vithu92/deep_divers"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/vithu92/deep_divers/issues">Report Bug</a>
    ·
    <a href="https://github.com/vithu92/deep_divers/issues">Request Feature</a>
    <br />
    <p>Team: Deep Divers</p>
    <p>Authors:</p>
    <p>Ezgi Köker Gökgöl</p>
    <p>Vithushan Mahendran</p>
    <p>Alexandra Alinka Zimmermann</p>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

<p align="justify">For this project, we target Pop-Up restaurants, where cooks have much freedom to try new food and meal concepts. They can indeed change their menus very easily.
For the business part, the goal of this project is to help restaurant owners to improve their business. First, we want to support them in the design of their menu and in the choice of their meals based on the population’s food taste. To do so, we will use a Google Trends API. Second, thanks to Edamam API, we will suggest them various recipes which are based on these meals, diet types, or ingredients preferences. Finally, we will also help them to plan their budget. Thus, we want to provide them with an estimated total cost of recipe and its ingredients. We fetch the ingredients price from the Coop website.
For the technical part, the objectives of our project are first to extract data from different data sources, to be precise, from two APIs, namely Google Trends and Edamam, and to scrape data from the Coop website. Second, we build a Data Lake system where we load the data extracted from the three data sources mentioned previously. In the second part of the project, we will build a Data Warehouse, where we will clean, analyze, and prepare our data to be able to answer our research questions. Finally, we will visualize our data using Tableau Desktop and provide access to our end-users in order to help them to make better decisions when designing their menus.</p>


<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [Python](https://www.python.org/)
* [AWS RDS](https://aws.amazon.com/rds/)
* [AWS Lambda](https://docs.aws.amazon.com/lambda/index.html)
* [AWS S3](https://aws.amazon.com/de/s3/)
* [EventBridge](https://aws.amazon.com/de/eventbridge/)
* [Amazon KMS](https://aws.amazon.com/de/kms/)
* [Amazon Secret Manager](https://aws.amazon.com/de/secrets-manager/)


<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This repository contains all scripts which is used within the AWS environment to run this project. To locally run these scripts, please install all needed python packages according to the requirement.txt file.

### Installation
After cloning this repository, make sure to create a new virtual environment.
With te following command, you create a venv with the name "venv"
* python
  ```sh
  python3 -m venv venv
  ```
Actiavte the venv 
* python
  ```sh
  source venv/bin/activate
  ```
Install all package needed for this project
* python
  ```sh
  pip install -r requirements.txt
  ```

Now you are ready to run each seperate python file to see for yourself, what functionalty these script do.
<p align="right">(<a href="#top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

Find all examples how to use the scripts within each data source directory.
<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

The goal of this module is to build up a proper data lake for a specific usecase.


<p align="right">(<a href="#top">back to top</a>)</p>
