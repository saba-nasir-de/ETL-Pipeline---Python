"""
Connection class contains all types of database connections that can be used through out the pipeline.
I am only making relevant methos and attributes for mysql connection, for now.
"""
import mysql.connector as msql
from mysql.connector import Error

class Connection():
    def __init__(self):
         self.mysql_connection = None
            
    def create_mysql_connection(self,username,password,ip,port):
        # url = 'mysql://{0}:{1}@{2}:{3}'.format(username,password,ip,port)
        try:
            self.mysql_connection = msql.connect(host=ip, user=username,  
                                password=password)#give ur username, password
        except Error as e:
            print("Error while connecting to MySQL", e)