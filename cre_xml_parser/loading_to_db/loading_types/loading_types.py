import pandas as pd
from .lists_loading_type import add_dict_row, \
    get_dict_row_from_field_names_and_filed_values, save_list_to_db
from .pandas_loading_type import add_df_row, \
    get_df_row_from_field_names_and_filed_values, save_df_to_db


LOADING_TYPES = {
    'PANDAS': {
        'ADD_ROW': add_df_row,
        'GET_ROW': get_df_row_from_field_names_and_filed_values,
        'CONTAINER_FOR_ROWS': pd.DataFrame,
        'SAVE_ROWS': save_df_to_db,
    },
    'LIST': {
        'ADD_ROW': add_dict_row,
        'GET_ROW': get_dict_row_from_field_names_and_filed_values,
        'CONTAINER_FOR_ROWS': list,
        'SAVE_ROWS': save_list_to_db,
    }
}
