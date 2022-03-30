from Connection import *
from Sourcing import *
from Transform_Load import *


def main():
	#connect to staging db
	mysql_conn = Connection()
	mysql_conn.create_mysql_connection("daud.saleemi", "daud@123", "10.36.25.74", "3306")
	#start Sourcing

	source_calls_data =  Sourcing("`smaple_data_saba`","`daud`",mysql_conn.mysql_connection)
	source_calls_data.load_csv_to_mysql("D:\\sample.csv")
	