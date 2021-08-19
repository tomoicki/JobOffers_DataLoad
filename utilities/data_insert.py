import pandas
import sqlalchemy.orm
import sqlalchemy.sql.schema
import shortuuid
import datetime


def is_already_for_list_like(dataframe: pandas.DataFrame,
                             iterator: int,
                             dataframe_column_name: str,
                             session: sqlalchemy.orm.session.Session,
                             schema_from: sqlalchemy.sql.schema) -> list:
    """Not working :(
    Wanted to improve function update_tables so it doesn't contain this repeated 'is_already' part."""
    temp_list = []
    for item in dataframe.loc[iterator, dataframe_column_name]:
        is_already = session.get(schema_from, item)
        if is_already is None:
            for column in schema_from.__table__.columns.keys():
                if column == dataframe_column_name:
                    temp_list.append(schema_from(column=item))
        else:
            temp_list.append(is_already)
    return temp_list


def stamp_expired(data_df: pandas.DataFrame,
                  session_maker: sqlalchemy.orm.session.sessionmaker,
                  job_offer_schema: sqlalchemy.sql.schema) -> None:
    """Checks if existing record in database is present in newly scraped data. If it isn't then it means it no longer exist
    on site, therefore is expired."""
    session = session_maker()
    #  create a list of all job offer_url from DataFrame
    data_df_urls = data_df['offer_url'].to_list()
    #  get all existing offers from Database
    existing_offers = session.query(job_offer_schema).all()

    for offer in existing_offers:
        if offer.offer_url not in data_df_urls:
            offer.expired = 'true'
            offer.expired_at = datetime.datetime.today()
    session.commit()


def update_tables(data_df: pandas.DataFrame,
                  session_maker: sqlalchemy.orm.session.sessionmaker,
                  job_offer_schema: sqlalchemy.sql.schema,
                  company_schema: sqlalchemy.sql.schema,
                  jobsite_schema: sqlalchemy.sql.schema,
                  location_schema: sqlalchemy.sql.schema,
                  experience_schema: sqlalchemy.sql.schema,
                  employment_type_schema: sqlalchemy.sql.schema,
                  skill_schema: sqlalchemy.sql.schema) -> None:
    """Function that takes all_data DataFrame, splits it, normalizes, creates relation tables and puts everything into Database.
    Same functionality as update_PostgreSQL_procedure.create_PostgreSQL but arguably smoother. Although still not without flaws."""
    session = session_maker()
    for i in data_df.index:
        if session.get(job_offer_schema, data_df.loc[i, 'offer_url']) is None:
            job_offer = job_offer_schema(offer_url=data_df.loc[i, 'offer_url'],
                                         title=data_df.loc[i, 'title'],
                                         b2b_min=data_df.loc[i, 'b2b_min'],
                                         b2b_max=data_df.loc[i, 'b2b_max'],
                                         permanent_min=data_df.loc[i, 'permanent_min'],
                                         permanent_max=data_df.loc[i, 'permanent_max'],
                                         mandate_min=data_df.loc[i, 'mandate_min'],
                                         mandate_max=data_df.loc[i, 'mandate_max'],
                                         expired=data_df.loc[i, 'expired'],
                                         expired_at=data_df.loc[i, 'expired_at'],
                                         scraped_at=data_df.loc[i, 'scraped_at'],
                                         )
            #  i would like to avoid all those "is_already" but without it sqlalchemy keeps adding duplicate rows
            #  and then screams that cannot add duplicate key, was working when id was primary key for each table
            #  but then it just kept adding duplicate values
            is_already = session.get(company_schema, data_df.loc[i, 'company'])
            if is_already is None:
                job_offer.company = company_schema(company=data_df.loc[i, 'company'],
                                                   company_size=data_df.loc[i, 'company_size'])
            else:
                job_offer.company = is_already

            is_already = session.get(jobsite_schema, data_df.loc[i, 'jobsite'])
            if is_already is None:
                job_offer.jobsite = jobsite_schema(jobsite=data_df.loc[i, 'jobsite'])
            else:
                job_offer.jobsite = is_already

            loc_list = []
            for loc in data_df.loc[i, 'location']:
                is_already = session.get(location_schema, loc)
                if is_already is None:
                    loc_list.append(location_schema(location=loc))
                else:
                    loc_list.append(is_already)
            # job_offer.location = is_already_for_list_like(data_df, i, 'location', session, location_schema)

            exp_list = []
            for exp in data_df.loc[i, 'experience']:
                is_already = session.get(experience_schema, exp)
                if is_already is None:
                    exp_list.append(experience_schema(experience=exp))
                else:
                    exp_list.append(is_already)
            job_offer.experience = exp_list

            emp_list = []
            for emp in data_df.loc[i, 'employment_type']:
                is_already = session.get(employment_type_schema, emp)
                if is_already is None:
                    emp_list.append(employment_type_schema(employment_type=emp))
                else:
                    emp_list.append(is_already)
            job_offer.employment_type = emp_list

            skill_must_list = []
            for sm in data_df.loc[i, 'skills_must']:
                is_already = session.get(skill_schema, sm)
                if is_already is None:
                    skill_must_list.append(skill_schema(skill=sm))
                else:
                    skill_must_list.append(is_already)
            job_offer.skill_must = skill_must_list

            skill_nice_list = []
            for sn in data_df.loc[i, 'skills_nice']:
                is_already = session.get(skill_schema, sn)
                if is_already is None:
                    skill_nice_list.append(skill_schema(skill=sn))
                else:
                    skill_nice_list.append(is_already)
            job_offer.skill_nice = skill_nice_list

            session.add(job_offer)
            # print(f"Added {data_df.loc[i, 'title']}")
        session.commit()
