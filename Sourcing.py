"""
Sourcing class is responsible for extracting data from the source systems and load it into the staging area.
Then from the the Stazging Area, it could later be used for transformation of the loaded data.
In the current scenerio, We are loading data from csv files only
"""
# import OS module
import os
import pandas as pd

class Sourcing:
    def __init__(self,staging_table_name,staging_table_schema,connection):
        self.staging_table_name = staging_table_name
        self.staging_table_schema = staging_table_schema
        #this is mysql_connection that is made in the main class
        self.connection = connection
    
    def row_wise_load(self,chunk):
        if self.connection.is_connected():
            cursor = self.connection.cursor()
             #loop through the data frame
            for i,row in chunk.iterrows():
                #here %S means string values 
                sql = "INSERT INTO "+self.staging_table_schema+"."+self.staging_table_name+" (`year`, `industry_aggregation_nzsioc`,`industry_code_nzsioc`,`industry_name_nzsioc`,`units`,`variable_code`,`variable_name`,`variable_category`,`value`,`industry_code_anzsic06`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not auto committed by default, so we must commit to save our changes
                self.connection.commit()
                print("Chunk No :" +str(no)+" inserted")
        else:
            print("not connected to MySQL database")
        
    def bulk_load(self,chunk):
        if self.connection.is_connected():
            cursor = self.connection.cursor()
             #store each row as a tuple inside a list
            data = [tuple(row) for i,row in chunk.iterrows()]
            stmt= "INSERT INTO "+self.staging_table_schema+"."+self.staging_table_name+" (`year`, `industry_aggregation_nzsioc`,`industry_code_nzsioc`,`industry_name_nzsioc`,`units`,`variable_code`,`variable_name`,`variable_category`,`value`,`industry_code_anzsic06`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            #executemany , bulk loads into data base
            cursor.executemany(stmt, data)  
            self.connection.commit()
        else:
            print("not connected to MySQL database")
    
    def tuncate_staging_table(self):
        if self.connection.is_connected():
            cursor = self.connection.cursor()
            query = 'Truncate Table {0}.{1};'.format(self.staging_table_schema,self.staging_table_name)
            cursor.execute(query)
            return True
        else:
            return False
        
    def read_csv_to_mysqlstaging(self, file_name):    
        chunk_count = 0
        #truncate the staging table
         # creating thread
        thread_list = list() 
        for chunk in pd.read_csv(file_name, chunksize=10000 , header = 0, error_bad_lines=False,
                    warn_bad_lines=True,encoding="utf-8"):
            chunk_count = chunk_count + 1
            if(chunk_count == 1):
                
                # get header column
                column_list = chunk.columns
                
                # pre process the column names
                column_names = [name.lower() for name in column_list]
                
                # cast each column into string
                chunk = chunk.astype(str)
                
                #pass that function that dumps the chunk into the staging table to a thread
                #thread = threading.Thread(target=self.dump_to_staging, args=(chunk,))
                #thread.start()
                #thread_list.append(thread)
                #cant use multi threading , to avoid locks and dead locks
                self.bulk_load(chunk)
                print("Chunk No: "+str(chunk_count)+" has been loaded")
            else:
                # cast each column into string
                
                chunk = chunk.astype(str)
                #dump_to_staging(chunk)
                #dump it into a staging table
                #thread = threading.Thread(target=self.dump_to_staging, args=(chunk,), daemon=True)
                #thread.start()
                #thread_list.append(thread)
                #cant use multi threading , to avoid locks and dead locks
                self.bulk_load(chunk)
                print("Chunk No: "+str(chunk_count)+" has been loaded")
        
            
    #loads a single file into staging
    def load_csv_to_mysql(self,file_path): 
        if(self.tuncate_staging_table()):
            print("Staging Table Truncated")
            self.read_csv_to_mysqlstaging(file_path)
            
        else:
            print("Staging table is not truncated")