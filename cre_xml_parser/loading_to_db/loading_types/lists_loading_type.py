import sqlalchemy
import threading
from ...secondary_functions import get_dict_from_lists
from ..execute_insert_query import execute_insert_query


def add_dict_row(table_name_to_rows_dict: dict,
                 table_name: str,
                 row_in_dict_to_append: dict):
    table_name_to_rows_dict[table_name].append(row_in_dict_to_append)


def get_dict_row_from_field_names_and_filed_values(field_names: list,
                                                   field_values: list):
    return get_dict_from_lists(keys_list=field_names, values_list=field_values)


def save_list_to_db(list_of_rows: list,
                    tag: str,
                    table_name: str,
                    engine: sqlalchemy.engine.base.Engine,
                    threads: list,
                    tag_to_row_names_to_db_names_dict: dict):
    if len(list_of_rows) > 0:
        list_of_column_names = list(tag_to_row_names_to_db_names_dict[tag].keys())
        list_of_str_for_values = []
        for row_dict in list_of_rows:
            row_to_append_in_str = ','.join(list(
                map(lambda x: str(row_dict.get(x)) if row_dict.get(x) is not None else 'Null', list_of_column_names)))
            list_of_str_for_values.append("(" + row_to_append_in_str + ")")
        insert_query = "INSERT INTO {} VALUES {}".format(table_name, ','.join(list_of_str_for_values))
        thread = threading.Thread(target=execute_insert_query, args=(engine, insert_query, table_name, len(list_of_rows)))
        thread.start()
        threads.append(thread)
