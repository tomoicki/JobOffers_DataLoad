from os import environ as env
from dotenv import load_dotenv
import pandas
from utilities.update_MongoDB_procedure import check_update_or_create_MongoDB
from utilities.update_PostgreSQL_procedure import check_update_or_create_PostgreSQL

load_dotenv()
pandas.options.mode.chained_assignment = None  # default='warn'

all_data = pandas.read_pickle('example_data.pkl')
#  sending data to databases
#  both check_update_or_create need pandas.DataFrame with all data as first argument and database credentials.
#  normally, all_data should be a product of both_repair_procedure function from DataTransformation part of our project.
#  As our project is split into separate repositories we cannot directly use DataExtraction functions
#  so to show how the program works we provided example_data.pkl file.
check_update_or_create_MongoDB(all_data, env['mongoDB_host'], env['mongoDB_port'],
                               env['mongoDB_db_name'], env['mongoDB_collection_name'])
check_update_or_create_PostgreSQL(all_data, env['PostgreSQL_host'], env['PostgreSQL_port'], env['PostgreSQL_user'],
                                  env['PostgreSQL_password'], env['PostgreSQL_db_name'])

