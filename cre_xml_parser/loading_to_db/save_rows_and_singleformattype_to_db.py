import sqlalchemy
from typing import Callable
from ..constants import TAGS, get_table_name_by_tag
from .save_singleformattype_to_db import save_singleformattype_to_db


def save_rows_and_singleformattype_to_db(table_name_to_rows_dict: dict,
                                         hjids: list,
                                         prefix: str,
                                         engine: sqlalchemy.engine.base.Engine,
                                         save_rows_to_db: Callable,
                                         threads: list,
                                         tag_to_row_names_to_db_names_dict: dict):
    for tag in TAGS:
        table_name = get_table_name_by_tag(tag)
        save_rows_to_db(table_name_to_rows_dict[table_name], tag, prefix+table_name,
                        engine, threads, tag_to_row_names_to_db_names_dict)
    save_singleformattype_to_db(hjids, prefix, engine, threads)
