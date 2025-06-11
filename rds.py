import logging
import os
from sqlalchemy import create_engine, text, MetaData
import pandas as pd
#from utils import LeximGPTUtils

class LeximGPTRDS:

    def __init__(self):
        self.rds_connection_str = None
        self.rds_database_name = None
        self.set_rds_connection_str()

    def set_rds_connection_str(self):
        
        rds_host = os.getenv("AWS_RDS_DEV_HOST")
        rds_port = '3306'
        rds_user = os.getenv("AWS_RDS_DEV_USER")
        rds_password = os.getenv("AWS_RDS_DEV_USER_PASS")
        rds_database_name = os.getenv("AWS_RDS_DATABASE_NAME")
        self.rds_database_name = rds_database_name

        # rds_host = os.getenv("AWS_RDS_PROD_HOST")
        # rds_port = '3306'
        # rds_user = os.getenv("AWS_RDS_PROD_USER")
        # rds_password = os.getenv("AWS_RDS_PROD_USER_PASS")
        # rds_database_name = os.getenv("AWS_RDS_DATABASE_NAME")

        self.rds_connection_str = f'mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_database_name}'

    def get_rds_connection_str(self):
        return self.rds_connection_str

    def replace_in_rds(self, df_to_save, table_name):

        conn_str = self.get_rds_connection_str()
        engine = create_engine(conn_str)    

        # Write the DataFrame to RDS
        try:
            df_to_save.to_sql(name=table_name,  index=False, schema=self.rds_database_name,   con=engine, if_exists='replace')                                                     
            return True
        
        except Exception as e:
            logging.error (e)
            return False   
        finally:
            # Closing the connection
            engine.dispose()

    def save_to_rds(self, df_to_save, table_name):

        conn_str = self.get_rds_connection_str()
        engine = create_engine(conn_str)    

        # Write the DataFrame to RDS
        try:
            df_to_save.to_sql(name=table_name,  index=False, schema=self.rds_database_name,   con=engine, if_exists='append')                                                     
            return True
        
        except Exception as e:
            logging.error (e)
            return False   
        finally:
            # Closing the connection
            engine.dispose()

    def run_query(self, query_str):
        
        conn_str = self.get_rds_connection_str()
        engine = create_engine(conn_str)  

        try:
            with engine.connect() as conn:
                result = conn.execute(text(query_str))
                return True, result
        
        except Exception as e:
            logging.error(e)
            return False, None
        finally:
            engine.dispose()

    def run_query_to_df(self, query_str):
        
        conn_str = self.get_rds_connection_str() 
        engine = create_engine(conn_str)  
    
        try:
            with engine.connect() as conn:
                result = pd.read_sql_query(text(query_str), conn)
                return True, result
        
        except Exception as e:
            logging.error(e)
    
            return False, None
        finally:
            engine.dispose()

    def run_query_to_json(self, query_str):
    
        conn_str = self.get_rds_connection_str() 
        engine = create_engine(conn_str)  
 
        try:
            with engine.connect() as conn:
                result = pd.read_sql_query(text(query_str), conn)
                result = result.to_json(orient="records")
                return True, result

        except Exception as e:
            logging.error(e)

            return False, None
        
        finally:
            engine.dispose()

    def does_object_exist(self, object_id, itm_id, object_type):

        table_name = LeximGPTUtils.get_table_for_object_type(object_type)
                
        query_str = f"SELECT objectId FROM {table_name} WHERE objectId = \"{object_id}\" OR id = \"{itm_id}\""
    
        was_success, res = self.run_query(query_str)
        
        if was_success == True:
            rows = res.all()   
            if len(rows) == 0:           
                return False
            else:
                return True        
        else:
            logging.error("Run Query failed. Unable to determine if object exists.")
            raise Exception ("Run Query failed. Unable to determine if docket exists.")

    @staticmethod    
    def rename_columns(df):
        for col in df.columns:
            if col.find("attributes.") != -1:
                names = col.split(".")           
                df = df.rename(columns={col : names[1]})
        return df
