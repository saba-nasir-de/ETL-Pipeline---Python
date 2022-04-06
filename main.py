from Connection import *
from Sourcing import *
from Transform_Load import *
import json

def main():

	# load credentials from a json file
	credentials_file = open('context.json')
	credentials = json.load(credentials_file)
	username,password,ip,port,file_path  = credentials["username"],credentials["password"],credentials["ip"],credentials["port"],credentials["file_path"]
		
	#connect to staging db
	mysql_conn = Connection()
	mysql_conn.create_mysql_connection(username, password, ip, port)
	
	#load context
	staging_table_name,staging_table_schema,file_path  = credentials["staging_table_name"],credentials["staging_table_schema"],credentials["file_path"]
	#start Sourcing
	source_calls_data =  Sourcing(staging_table_name,staging_table_schema ,mysql_conn.mysql_connection)
	print("Data loading started...")
	source_calls_data.load_csv_to_mysql(file_path)
	print("Data loading completed!!")
	
	
	
	#start transforming and loading data to final table
	transform = Transform_Load(source_calls_data.staging_table_name,source_calls_data.staging_table_schema,mysql_conn.mysql_connection)
	print("Data transformation started!!")
	transform.transform_Load_sampledata()
	print("Data transformation and loading to final table completed!!")

	
if __name__=="__main__":
    main()