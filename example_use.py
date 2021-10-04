import pandas
from os import environ as env
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from job_offers_data_load.postgre_sql_connection_functions import make_connection_to_postgresql
from job_offers_data_load.postgre_sql_data_insert import update_tables, stamp_expired
from job_offers_data_load.postgre_sql_tables_declaration import *
from job_offers_data_load.update_mongo_db_procedure import check_update_or_create_MongoDB

load_dotenv()
pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', None)
pandas.set_option('display.max_colwidth', 100)

pandas.options.mode.chained_assignment = None  # default='warn'

#  create connection with PostgreSQL
cnx = make_connection_to_postgresql(env['PostgreSQL_host2'], env['PostgreSQL_port2'], env['PostgreSQL_user2'],
                                    env['PostgreSQL_password2'], env['PostgreSQL_db_name2'])

#  create all declared tables inside DB
#  if Tables already exist, it wont have any effect
Base.metadata.create_all(cnx)

#  create session
Session = sessionmaker(bind=cnx)
s = Session()

#  normally, we would get all_data as a product of both_repair_procedure function from DataTransformation part of our project.
#  As our project is split into separate repositories we cannot directly use DataExtraction functions
#  so to show how the program works we provided example_data.pkl file.
all_data = pandas.read_pickle('example_data.pkl')

#  sending data to PostgreSQL
update_tables(all_data, Session)
# stamp_expired(all_data, Session)

#  sending data to MongoDB
# check_update_or_create_MongoDB(all_data, env['mongoDB_host'], env['mongoDB_port'],
#                                env['mongoDB_db_name'], env['mongoDB_collection_name'])
