import pandas
from utilities import PostgreSQL_connection_functions, normalization_functions
from sqlalchemy import inspect


def check_update_or_create_PostgreSQL(data_df: pandas.DataFrame, host: str, port: str, user: str, password: str, db_name: str) -> None:
    """Checks if database has any Tables, if not calls create_PostgreSQL, if yes calls update_PostgreSQL."""
    #  create connection with PostgreSQL
    cnx = PostgreSQL_connection_functions.connection2db(host, port, user, password, db_name)
    inspector = inspect(cnx)
    if len(inspector.get_table_names()) == 0:
        create_PostgreSQL(data_df, host, port, user, password, db_name)
    elif len(inspector.get_table_names()) > 0:
        update_PostgreSQL(data_df, host, port, user, password, db_name)
    else:
        print("Something went wrong.")


def create_PostgreSQL(data_df: pandas.DataFrame, host: str, port: str, user: str, password: str, db_name: str) -> None:
    """Create new database. Normalization included."""
    #  split data into smaller separate DataFrames as a step to normalization
    location_df, company_df, experience_df, employment_type_df, skill_df, expired_df, jobsite_df = \
        normalization_functions.split_data(data_df)
    #  add new indexes to have it easier in PostgreSQL
    normalization_functions.add_uuid(data_df, 'job_offer_uuid')
    normalization_functions.add_uuid(location_df, 'location_uuid')
    normalization_functions.add_uuid(company_df, 'company_uuid')
    normalization_functions.add_uuid(experience_df, 'experience_uuid')
    normalization_functions.add_uuid(employment_type_df, 'employment_type_uuid')
    normalization_functions.add_uuid(skill_df, 'skill_uuid')
    normalization_functions.add_uuid(expired_df, 'expired_uuid')
    normalization_functions.add_uuid(jobsite_df, 'jobsite_uuid')
    #  create DataFrame that will become our main JobOffers table in DB
    offers_df = data_df[['job_offer_uuid', 'title', 'b2b_min', 'b2b_max', 'permanent_min', 'permanent_max',
                         'mandate_min', 'mandate_max', 'expired', 'expired_at', 'scraped_at', 'offer_url']]
    #  adding n-to-one columns from corresponding Table already as id and not as name
    offers_df['company_uuid'] = ''
    offers_df['jobsite_uuid'] = ''
    offers_df = normalization_functions.add_n_to_one(data_df, offers_df, company_df, 'company', 'company_uuid')
    offers_df = normalization_functions.add_n_to_one(data_df, offers_df, jobsite_df, 'jobsite', 'jobsite_uuid')
    #  creation of relation tables
    job_offers_2_location = normalization_functions.create_relation_table(
        data_df, location_df, 'job_offer_uuid', 'location', 'location_uuid', 'location', 'job_offer_uuid',
        'location_uuid')
    job_offers_2_experience = normalization_functions.create_relation_table(
        data_df, experience_df, 'job_offer_uuid', 'experience', 'experience_uuid', 'experience', 'job_offer_uuid',
        'experience_uuid')
    job_offers_2_employment_type = normalization_functions.create_relation_table(
        data_df, employment_type_df, 'job_offer_uuid', 'employment_type', 'employment_type_uuid',
        'employment_type', 'job_offer_uuid', 'employment_type_uuid')
    job_offers_2_skills_must = normalization_functions.create_relation_table(
        data_df, skill_df, 'job_offer_uuid', 'skills_must', 'skill_uuid', 'skill', 'job_offer_uuid', 'skill_uuid')
    job_offers_2_skills_nice = normalization_functions.create_relation_table(
        data_df, skill_df, 'job_offer_uuid', 'skills_nice', 'skill_uuid', 'skill', 'job_offer_uuid', 'skill_uuid')
    #  send DataFrames to remote PostgreSQL DB as tables, we do this step only once at beginning
    #  if u try to run this script when Databases are already present on remote then it will
    #  replace them if there were no keys defined or
    #  raise an error cuz pandas.to_sql doesnt have the power to override existing keys
    PostgreSQL_connection_functions.put_df_into_remote_db(
        data_df, 'All', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        offers_df, 'JobOffers', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        location_df, 'Location', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        company_df, 'Company', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        experience_df, 'Experience', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        employment_type_df, 'Employment_type', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        skill_df, 'Skill', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        expired_df, 'Expired', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        jobsite_df, 'Jobsite', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        job_offers_2_location, 'JobOffers_to_Location', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        job_offers_2_experience, 'JobOffers_to_Experience', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        job_offers_2_employment_type, 'JobOffers_to_Employment_Type', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        job_offers_2_skills_must, 'JobOffers_to_Skill_Must', host, port, user, password, db_name, 'replace')
    PostgreSQL_connection_functions.put_df_into_remote_db(
        job_offers_2_skills_nice, 'JobOffers_to_Skill_Nice', host, port, user, password, db_name, 'replace')
    print("Finished creating PostgreSQL.")


def update_PostgreSQL(new_data: pandas.DataFrame, host: str, port: str, user: str, password: str, db_name: str) -> None:
    """Updates existing database. Decides which records are new, which are obsolete and stamps accordingly."""
    #  get data from remote PostgreSQL DB
    old_data = PostgreSQL_connection_functions.get_from_remote_db('All', host, port, user, password, db_name)
    location_df = PostgreSQL_connection_functions.get_from_remote_db('Location', host, port, user, password, db_name)
    company_df = PostgreSQL_connection_functions.get_from_remote_db('Company', host, port, user, password, db_name)
    experience_df = PostgreSQL_connection_functions.get_from_remote_db('Experience', host, port, user, password, db_name)
    employment_type_df = PostgreSQL_connection_functions.get_from_remote_db('Employment_type', host, port, user, password, db_name)
    skill_df = PostgreSQL_connection_functions.get_from_remote_db('Skill', host, port, user, password, db_name)
    jobsite_df = PostgreSQL_connection_functions.get_from_remote_db('Jobsite', host, port, user, password, db_name)
    #  intersection of two DataFrames gives rows which are the same in both
    intersection = old_data.merge(new_data[['offer_url']])
    #  expireds DataFrame contains rows which exist in remote but are no longer in fresh data
    #  which means they were taken off the nofluff.com meaning they expired
    expireds = old_data[old_data['offer_url'].isin(intersection['offer_url']) == False]
    #  expireds have to be appended, but we already have this job offers each with primary key
    #  so it will be impossible to just append it using pandas.to_sql
    #  we have to directly inject query to database which just replaces 'expired' value from False to True
    #  and also add a timestamp of expiration
    cnx = PostgreSQL_connection_functions.connection2db(host, port, user, password, db_name)
    for i in range(len(expireds)):
        idd = expireds.loc[expireds.index[i], 'job_offer_uuid']
        #  update All
        query_to_All = "UPDATE \"All\" SET expired = 'true', expired_at = current_date WHERE job_offer_uuid = %s"
        cnx.execute(query_to_All, idd)
        #  update JobOffers
        query_to_JobOffers = "UPDATE \"JobOffers\" SET expired = 'true', expired_at = current_date WHERE job_offer_uuid = %s"
        cnx.execute(query_to_JobOffers, idd)
    #  news DataFrame contains rows which only exist in fresh data, they were not found in DB
    #  meaning these are new job offers which will have to be handled similar to what happened in 'create_database.py'
    #  they will have to be normalized, in case new value of employer, location, technology appears - it will have
    #  to be detected and new record given uuid and appended to corresponding table
    news = new_data[new_data['offer_url'].isin(intersection['offer_url']) == False]
    if len(news) > 0:
        #  split data into smaller separate DataFrames as a step to normalization
        new_location_df, new_company_df, new_experience_df, new_employment_type_df, new_skill_df, new_expired_df, new_jobsite_df = \
            normalization_functions.split_data(news)
        #  add new indexes to have it easier in PostgreSQL
        normalization_functions.add_uuid(news, 'job_offer_uuid')
        #  create DataFrame that will become our main JobOffers table in DB
        new_offers_df = news[['job_offer_uuid', 'title', 'b2b_min', 'b2b_max', 'permanent_min',
                              'permanent_max', 'mandate_min', 'mandate_max', 'expired_at', 'scraped_at', 'offer_url']]
        #  updating old DFs by adding new uuid if record did not exist before
        updated_location_df, new_location_df = \
            normalization_functions.update_uuid(new_location_df, location_df, 'location', 'location_uuid')
        updated_company_df, new_company_df = \
            normalization_functions.update_uuid(new_company_df, company_df, 'company', 'company_uuid')
        updated_experience_df, new_experience_df = \
            normalization_functions.update_uuid(new_experience_df, experience_df, 'experience', 'experience_uuid')
        updated_employment_type_df, new_employment_type_df = \
            normalization_functions.update_uuid(new_employment_type_df, employment_type_df,
                                                'employment_type', 'employment_type_uuid')
        updated_skill_df, new_skill_df = \
            normalization_functions.update_uuid(new_skill_df, skill_df, 'skill', 'skill_uuid')
        updated_jobsite_df, new_jobsite_df = \
            normalization_functions.update_uuid(new_jobsite_df, jobsite_df, 'jobsite', 'jobsite_uuid')
        #  adding n-to-one columns from corresponding Table already as id and not as name
        new_offers_df = normalization_functions.add_n_to_one(news, new_offers_df, updated_company_df, 'company', 'company_uuid')
        new_offers_df = normalization_functions.add_n_to_one(news, new_offers_df, updated_jobsite_df, 'jobsite', 'jobsite_uuid')
        #  creation of relation tables
        job_offers_2_location = normalization_functions.create_relation_table(
            news, updated_location_df, 'job_offer_uuid', 'location', 'location_uuid',
            'location', 'job_offer_uuid', 'location_uuid')
        job_offers_2_experience = normalization_functions.create_relation_table(
            news, updated_experience_df, 'job_offer_uuid', 'experience', 'experience_uuid',
            'experience', 'job_offer_uuid', 'experience_uuid')
        job_offers_2_employment_type = normalization_functions.create_relation_table(
            news, updated_employment_type_df, 'job_offer_uuid', 'employment_type', 'employment_type_uuid',
            'employment_type', 'job_offer_uuid', 'employment_type_uuid')
        job_offers_2_skills_must = normalization_functions.create_relation_table(
            news, updated_skill_df, 'job_offer_uuid', 'skills_must', 'skill_uuid', 'skill', 'job_offer_uuid', 'skill_uuid')
        job_offers_2_skills_nice = normalization_functions.create_relation_table(
            news, updated_skill_df, 'job_offer_uuid', 'skills_nice', 'skill_uuid', 'skill', 'job_offer_uuid', 'skill_uuid')
        #  send new job offers to DB
        PostgreSQL_connection_functions.put_df_into_remote_db(
            news, 'All', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_offers_df, 'JobOffers', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_location_df, 'Location', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_company_df, 'Company', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_experience_df, 'Experience', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_employment_type_df, 'Employment_type', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_skill_df, 'Skill', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            new_jobsite_df, 'Jobsite', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            job_offers_2_location, 'JobOffers_to_Location', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            job_offers_2_experience, 'JobOffers_to_Experience', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            job_offers_2_employment_type, 'JobOffers_to_Employment_Type', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            job_offers_2_skills_must, 'JobOffers_to_Skill_Must', host, port, user, password, db_name, 'append')
        PostgreSQL_connection_functions.put_df_into_remote_db(
            job_offers_2_skills_nice, 'JobOffers_to_Skill_Nice', host, port, user, password, db_name, 'append')
        print("Finished updating PostgreSQL.")
    else:
        print("No new records provided")

