import pandas as pd
import sqlalchemy
from ...constants import TAGS, get_table_name_by_tag


def get_tag_to_row_names_to_db_names_dict(engine: sqlalchemy.engine.base.Engine):
    result_dict = {}
    for tag in TAGS:
        table_name = get_table_name_by_tag(tag)
        describe_query = "SELECT column_name, data_type " \
                         "FROM information_schema.columns " \
                         f"WHERE table_name = '{table_name}';"
        df = pd.read_sql_query(describe_query, engine)
        row_names_to_db_names_dict = {}
        for _, row in df[['column_name']].iterrows():
            row_names_to_db_names_dict[row['column_name'].lower().replace('_', '')] = row['column_name']
        result_dict[tag] = row_names_to_db_names_dict
    return result_dict
