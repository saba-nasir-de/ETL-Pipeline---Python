"""
In an ETL pipeline, Tranformations solely depends on the business needs, befor loading it to the final table
We'll do the following tranformations on our sample data table:
        1- Cast numeric columns, from strings to numbers
        2- Parsing date time columns
Transformations are performed via a stored procedure, which is executed using the Transform class.
After the Transformtions, the data is loaded into the Final Sample Table, ready to be used for analysis and reporting.

"""

class Tranform_Load():
    
    def __init(self,staging_table_name,staging_table_schema,connection):
        self.staging_table_name = staging_table_name
        self.staging_table_schema = staging_table_schema
        self.connection = connection
    
    def transform_Load_sampledata(self):
        # connect to server
        cursor = self.connection.cursor()
        cursor.callproc('subscription_dwh.p_update_facts')
        print('Table transformed and Loaded into finaal tables.')

        # commit & close connection
        cursor.close()
        self.connection.commit()
        self.connection.close()
        print('Disconnected from database.')

      