import pandas as pd
from .lists_loading_type import add_dict_row, \
    get_dict_row_from_field_names_and_filed_values, save_list_to_db
from .pandas_loading_type import add_df_row, \
    get_df_row_from_field_names_and_filed_values, save_df_to_db


LOADING_TYPE_TO_ADD_ROW_FUNCTION = {
    'PANDAS': add_df_row,
    'LIST': add_dict_row,
}

LOADING_TYPE_TO_GET_ROW_FROM_FIELD_NAMES_AND_FIELD_VALUES_FUNCTION = {
    'PANDAS': get_df_row_from_field_names_and_filed_values,
    'LIST': get_dict_row_from_field_names_and_filed_values,
}

LOADING_TYPE_TO_CONTAINER_FOR_ROWS_CONSTRUCTOR = {
    'PANDAS': pd.DataFrame,
    'LIST': list,
}

LOADING_TYPE_TO_SAVE_ROWS_TO_DB_FUNCTION = {
    'PANDAS': save_df_to_db,
    'LIST': save_list_to_db,
}
