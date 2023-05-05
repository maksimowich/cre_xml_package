#!/usr/bin/env python3
import sqlalchemy
from cre_xml_parser import recreate_tables_and_load_xml_files


def main():
    connection_string = 'postgresql+psycopg2://postgres:5555@db.mpkazantsev.ru/demo'
    engine = sqlalchemy.create_engine(connection_string)

    prefix = 'adm.ad2_sf'
    path_to_folder_with_xml_files = "C:\\Users\\cahr2\\GPB\\xml_parser\\xml_out"
    loading_size = 500
    loading_method = 'LIST'
    db_name = 'POSTGRES'

    recreate_tables_and_load_xml_files(connection_string,
                                       prefix,
                                       path_to_folder_with_xml_files,
                                       loading_size,
                                       loading_method,
                                       db_name)


if __name__ == '__main__':
    main()
