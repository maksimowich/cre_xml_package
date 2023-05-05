import os
import sqlalchemy
from sqlalchemy.exc import OperationalError
from ..constants import TABLE_NAMES, TAGS, get_table_name_by_tag
from .loading_types.loading_types import LOADING_TYPES
from ..parsing_algorithm import parse_xml_file
from .db_methods import DB_METHODS
from .save_rows_and_singleformattype_to_db import save_rows_and_singleformattype_to_db
from ..secondary_functions.time_decorator import timeit


def load_xml_files(engine: sqlalchemy.engine.base.Engine,
                   prefix: str,
                   path_to_folder_with_xml_files: str,
                   loading_size: int,
                   loading_method: str,
                   tag_to_row_names_to_db_names_dict):
    table_name_to_rows_dict = {}
    container_for_rows_constructor = LOADING_TYPES[loading_method]['CONTAINER_FOR_ROWS']
    for table_name in TABLE_NAMES:
        table_name_to_rows_dict[table_name] = container_for_rows_constructor()
        table_name_to_rows_dict[table_name + "_hjid"] = 0  # счетчик idшников внутри сущности

    threads = []
    save_rows_to_db = LOADING_TYPES[loading_method]['SAVE_ROWS']
    hjids = []
    tables_current_hjid = {}
    for filename in os.listdir(path_to_folder_with_xml_files):
        if filename.endswith(".xml"):
            path_to_xml_file = path_to_folder_with_xml_files + "/" + filename
            hjid = int(filename.split('.')[0])
            hjids.append(hjid)
            parse_xml_file(path_to_xml_file, hjid, table_name_to_rows_dict,
                           tables_current_hjid, loading_method, tag_to_row_names_to_db_names_dict)
            if len(hjids) == loading_size:
                save_rows_and_singleformattype_to_db(table_name_to_rows_dict, hjids, prefix, engine,
                                                     save_rows_to_db, threads, tag_to_row_names_to_db_names_dict)
                for tag in TAGS:
                    table_name_to_rows_dict[get_table_name_by_tag(tag)] = container_for_rows_constructor()  # очищаем
                hjids = []
    else:
        save_rows_and_singleformattype_to_db(table_name_to_rows_dict, hjids, prefix, engine,
                                             save_rows_to_db, threads, tag_to_row_names_to_db_names_dict)
    for thread in threads:
        thread.join()


def recreate_tables_and_load_xml_files(connection_string: str,
                                       prefix: str,
                                       path_to_folder_with_xml_files: str,
                                       loading_size: int,
                                       loading_type: str,
                                       db_name: str):
    if loading_type not in LOADING_TYPES.keys():
        print("Specified loading_type is not supported.")
    if db_name not in DB_METHODS.keys():
        print("Specified db_name is not supported.")
        return
    if not os.path.exists(path_to_folder_with_xml_files):
        print("Specified path to folder with xml files doesn't exists.")
        return
    engine = sqlalchemy.create_engine(connection_string)
    try:
        engine.connect()
    except OperationalError:
        print('Wrong connection string was specified.')
        return
    try:
        DB_METHODS[db_name]['RECREATE_TABLES'](prefix, engine)
    except:
        print('Something went wrong while recreating tables. Operation was rollbacked.')
        return
    tag_to_row_names_to_db_names_dict = DB_METHODS[db_name]['GET_NAMES_MAPPING'](engine)
    load_xml_files_timed = timeit(load_xml_files)
    load_xml_files_timed(engine,
                         prefix,
                         path_to_folder_with_xml_files,
                         loading_size,
                         loading_type,
                         tag_to_row_names_to_db_names_dict)
    print('All files has been loaded to DB.')
