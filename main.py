from Connection import *
from Sourcing import *
from Transform_Load import *
import json

def main():

	# load credentials froma json file
	# Opening JSON file
	credentials_file = open('data.json')
	 
	# returns JSON object as
	# a dictionary
	credentials = json.loads(credentials_file)
	
	
	#connect to staging db
	mysql_conn = Connection()
	mysql_conn.create_mysql_connection("daud.saleemi", "daud@123", "10.36.25.74", "3306")
	#start Sourcing

	source_calls_data =  Sourcing("`smaple_data_saba`","`daud`",mysql_conn.mysql_connection)
	print("Data loading started...")
	source_calls_data.load_csv_to_mysql("C:\\python_projects\\pipeline\\sample.csv")
	print("Data loading completed!!")
	
	#start tranforming an dloading data to final table
	transform = Transform_Load(source_calls_data.staging_table_name,source_calls_data.staging_table_schema,mysql_conn.mysql_connection)
	print("Data transformation started!!")
	transform.transform_Load_sampledata()
	print("Data transformation and loading to final table completed!!")
	
	#heyyy
	#hi
	
if __name__=="__main__":
    main()