# Building ETL Pipeline Using Python and MySQL Database
Its a an ETL pipeline build using python language. The Relational database used here is MySQL. The mproject provides a template for creating an ETL pipeline. This project can be customised to your own business case scenerios. I have passed a sample data set through the ETL pipeline to show case how this template can be used.

# ETL Pipeline
ETL, or extract, transform, and load, is a data engineering process that involves extracting data from various sources, transforming it into a usable and trustworthy resource, and loading it into systems that end users can access and use downstream to address business problems. In this project, we expect to extract csv files that are places at the given loaction and tranform them according to our business need into MySQL Datse tables.

# Dataset Summary
The dataset used in my project is Annual Enterprise Survey and it could have changed in the meantime. Attribution: Contains information provided by New Zealand Government licensed under the CC Attribution 4.0 International (CC BY 4.0).

The Annual Enterprise Survey provides statistics on the financial performance and financial position of New Zealand businesses, covering most areas of economic activity. Data for years back to 2004 are available from the source URL (https://datasetguide.com/dataset/4eabfa511f702ff75a2ca24c06ec2fe0/).

|  Dataset          	|   Annual Enterprise Survey	|
|---                	|---	                        |
|    Provider       	|   Stats NZ     	            |
|    Created            |   10/04/2017   	            |
|    Updated	        |   25/06/2020  	            |
|    Licence            |   License information is derived automatically. Refer to source for license information before using this data.	|
|    Country            |    New Zealand            	|

# ETL Template Walkthrough
The purpose of the template is provide basic working structure of ETL pipeline which can be costomised to relevant business scenerio. The data set used here is Annual Enterprise Survey, which is publically available. It is only used to demostrate that how this template can be used for your business scenerio.

*** 

* Conjext.json : Its a context file that can be used to fill in Database credentials and folder path to the Source files
* Connection.py : File contains the connection class that contain method to connect to MySQL Data base. For instance, the organisation wishes to change the databases, all we need to do is add a new .connection method to the class and we are good to use the same template for building an ETL pipeline.
* Sourcing.py : This file contain Sourcing class that reads data from files and load it to a staging are. The technique used to Load the data to the staging is the Bulk load. This ensures fast and efficient loading.
* Transform_Load.py : The file contain Tranformation class that contain a set of transformations that can be used to other sourced data as well. First convert It removes dublicates, modifies and cast string values according to the data types in the final table and handle nulls (handle them as per the business needs) . We can add more methods to the class depending on your needs.
* DDL.txt: contains the DDL of staging and final table.

***
