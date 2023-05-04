import os
import sqlalchemy
from typing import Callable
from ..constants import TABLE_NAMES, TAGS, get_table_name_by_tag
from .loading_types import LOADING_TYPE_TO_CONTAINER_FOR_ROWS_CONSTRUCTOR
from .loading_types import LOADING_TYPE_TO_SAVE_ROWS_TO_DB_FUNCTION
from ..parsing_algorithm import parse_xml_file


def load_xml_files(save_rows_and_singleformattype_to_db: Callable,
                   engine: sqlalchemy.engine.base.Engine,
                   prefix: str,
                   path_to_folder_with_xml_files: str,
                   loading_size: int,
                   loading_method: str):
    table_name_to_rows_dict = {}
    container_for_rows_constructor = LOADING_TYPE_TO_CONTAINER_FOR_ROWS_CONSTRUCTOR[loading_method]
    for table_name in TABLE_NAMES:
        table_name_to_rows_dict[table_name] = container_for_rows_constructor()
        table_name_to_rows_dict[table_name + "_hjid"] = 0  # счетчик idшников внутри сущности

    threads = []
    save_rows_to_db = LOADING_TYPE_TO_SAVE_ROWS_TO_DB_FUNCTION[loading_method]
    hjids = []
    tables_current_hjid = {}
    for filename in os.listdir(path_to_folder_with_xml_files):
        if filename.endswith(".xml"):
            path_to_xml_file = path_to_folder_with_xml_files + "/" + filename
            hjid = int(filename.split('.')[0])
            hjids.append(hjid)
            parse_xml_file(path_to_xml_file, hjid, table_name_to_rows_dict, tables_current_hjid, loading_method)
            if len(hjids) == loading_size:
                save_rows_and_singleformattype_to_db(table_name_to_rows_dict, hjids, prefix,
                                                     engine, save_rows_to_db, threads)
                for tag in TAGS:
                    table_name_to_rows_dict[get_table_name_by_tag(tag)] = container_for_rows_constructor()  # очищаем
                hjids = []
    else:
        save_rows_and_singleformattype_to_db(table_name_to_rows_dict, hjids, prefix,
                                             engine, save_rows_to_db, threads)

    for thread in threads:
        thread.join()


def recreate_tables_and_load_xml_files(recreate_tables: Callable,
                                       save_rows_and_singleformattype_to_db: Callable,
                                       engine: sqlalchemy.engine.base.Engine,
                                       prefix: str,
                                       path_to_folder_with_xml_files: str,
                                       loading_size: int,
                                       loading_method: str):
    recreate_tables(prefix, engine)
    load_xml_files(save_rows_and_singleformattype_to_db,
                   engine,
                   prefix,
                   path_to_folder_with_xml_files,
                   loading_size,
                   loading_method)
    print('All files has been loaded to DB.')
