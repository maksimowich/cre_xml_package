#!/usr/bin/env python3
import sqlalchemy
import cre_xml_parser
from cre_xml_parser import recreate_tables_and_load_xml_files, TAG_TO_TABLE_TYPES_DICT


def main():
    print('hello')
    print(type(recreate_tables_and_load_xml_files))
    print(TAG_TO_TABLE_TYPES_DICT)
    # connection_string = 'postgresql+psycopg2://postgres:5555@db.mpkazantsev.ru/demo'
    # engine = sqlalchemy.create_engine(connection_string)
    #
    # prefix = 'adm.ad_sf_'
    # path_to_folder_with_xml_files = 'C:\Users\cahr2\GPB\xml_parser'
    # loading_size = 500
    # loading_method = 'LIST'
    #
    # recreate_tables_and_load_xml_files(engine,
    #                                    prefix,
    #                                    path_to_folder_with_xml_files,
    #                                    loading_size,
    #                                    loading_method)

if __name__ == '__main__':
    main()
