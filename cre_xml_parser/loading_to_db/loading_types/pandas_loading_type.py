import sqlalchemy
import threading
import pandas as pd
from ..execute_insert_query import execute_insert_query


def add_df_row(table_name_to_rows_dict, table_name, row_in_df_to_append):
    table_name_to_rows_dict[table_name] = pd.concat([table_name_to_rows_dict[table_name], row_in_df_to_append],
                                                    ignore_index=True)


def get_df_row_from_field_names_and_filed_values(field_names, field_values):
    return pd.DataFrame(data=[field_values], columns=field_names)


def save_df_to_db(df_of_rows: pd.DataFrame,
                  tag: str,
                  table_name: str,
                  engine: sqlalchemy.engine.base.Engine,
                  threads: list,
                  tag_to_row_names_to_db_names_dict: dict):  # сохранение датафрема в таблицу
    if df_of_rows.shape[0] > 0:
        names_dict = tag_to_row_names_to_db_names_dict[tag]
        str_for_columns = ", ".join(map(lambda x: names_dict.get(x, x), df_of_rows.columns.values))
        values_list = []
        for _, row in df_of_rows.iterrows():
            values_list.append("(" + ", ".join(map(str, row)).replace("nan", "NULL") + ")")
        insert_query = "INSERT INTO {} ({}) VALUES {}".format(table_name, str_for_columns, ",".join(values_list))
        thread = threading.Thread(target=execute_insert_query, args=(engine, insert_query, table_name, df_of_rows.shape[0]))
        thread.start()
        threads.append(thread)
