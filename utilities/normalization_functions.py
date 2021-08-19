import pandas
import shortuuid

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', None)
pandas.set_option('display.max_colwidth', 100)
pandas.options.mode.chained_assignment = None


def add_n_to_one(all_df: pandas.DataFrame, main_df: pandas.DataFrame, to_df: pandas.DataFrame,
                 column_name: str, uuid_column_name: str) -> pandas.DataFrame:
    """Adds N-to-one column to main_df."""
    all_df.reset_index(drop=True, inplace=True)
    for i in range(len(all_df.index)):
        name = all_df.loc[i, column_name]
        idd = to_df.loc[to_df[column_name] == name][uuid_column_name].item()
        index = main_df.loc[main_df['job_offer_uuid'] == all_df.loc[i, 'job_offer_uuid']].index.item()
        main_df.loc[index, uuid_column_name] = idd
    return main_df


def create_relation_table(big_df: pandas.DataFrame,
                          unique_df: pandas.DataFrame,
                          big_df_id_col_name: str,
                          big_df_item_col_name: str,
                          unique_df_id_col_name: str,
                          unique_df_item_col_name: str,
                          new_df_id_col_name: str,
                          new_df_item_col_name: str) -> pandas.DataFrame:
    """Takes DataFrame that stores all information (big_df), takes DataFrame with unique values (unique_df)
    and creates DataFrame that represents relation between column from big_df and columns from unique_df.
    kwarg int32 decides if new_df_item_col_name will be converted to int (sometimes needed as pandas makes it float
    and it is going to be id column in postgreSQL so needs to be an int."""
    big_df_id_list = []
    big_df_item_id_list = []
    for i in range(len(big_df.index)):
        big_df_id = big_df.loc[i, big_df_id_col_name]
        big_df_item_list = big_df.loc[i, big_df_item_col_name]
        if len(big_df_item_list) == 0:
            big_df_id_list.append(big_df_id)
            big_df_item_list.append(None)
        else:
            for j in range(len(big_df_item_list)):
                big_df_id_list.append(big_df_id)
                big_df_item_id = unique_df.loc[unique_df[unique_df_item_col_name] ==
                                               big_df_item_list[j]][unique_df_id_col_name].item()
                big_df_item_id_list.append(big_df_item_id)
    big_df_to_unique_df = pandas.DataFrame(list(zip(big_df_id_list, big_df_item_id_list)),
                                           columns=[new_df_id_col_name, new_df_item_col_name])
    return big_df_to_unique_df


def add_uuid(df: pandas.DataFrame, column_name: str) -> pandas.DataFrame:
    """Adds new column to dataFrame with unique id that will work as primary key in DB."""
    df[column_name] = 0
    df[column_name] = df[column_name].map(lambda x: shortuuid.uuid())
    return df


def update_uuid(new_df: pandas.DataFrame,
                old_df: pandas.DataFrame,
                old_df_column_name: str,
                old_df_id_column_name: str):
    """Takes new_df and adds uuids if it haven't already existed in old_df.
    Then appends new rows to old_df and returns updated old_df."""
    intersection = old_df.merge(new_df[[old_df_column_name]])
    didnt_exits_before = new_df[new_df[old_df_column_name].isin(intersection[old_df_column_name]) == False]
    add_uuid(didnt_exits_before, old_df_id_column_name)
    updated_old_df = pandas.concat([didnt_exits_before, old_df], ignore_index=True)
    return updated_old_df, didnt_exits_before


def split_data(df: pandas.DataFrame):
    """Creation of all DFs that will become postgreSQL tables."""
    loc = df['location'].sum()
    locations = set(loc)
    location_df = pandas.DataFrame(locations, columns=['location'])
    company_df = df[['company', 'company_size']]
    company_df = company_df.drop_duplicates(subset=['company'], keep='first')
    exp = df['experience'].sum()
    experience = set(exp)
    experience_df = pandas.DataFrame(experience, columns=['experience'])
    emp = df['employment_type'].sum()
    emp_types = set(emp)
    employment_type_df = pandas.DataFrame(emp_types, columns=['employment_type'])
    skill = df['skills_must'].sum() + df['skills_nice'].sum()
    skills = set(skill)
    skill_df = pandas.DataFrame(skills, columns=['skill'])
    expire = ['true', 'false']
    expired_df = pandas.DataFrame(expire, columns=['expired'])
    job = df['jobsite'].to_list()
    jobsites = set(job)
    jobsite_df = pandas.DataFrame(jobsites, columns=['jobsite'])
    return location_df, company_df, experience_df, employment_type_df, skill_df, expired_df, jobsite_df
