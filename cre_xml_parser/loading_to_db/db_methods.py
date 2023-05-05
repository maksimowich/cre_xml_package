from .postgres.recreate_tables  import recreate_tables
from .postgres.get_tag_to_row_names_to_db_names_dict import get_tag_to_row_names_to_db_names_dict


DB_METHODS = {
    'POSTGRES': {
        'RECREATE_TABLES': recreate_tables,
        'GET_NAMES_MAPPING': get_tag_to_row_names_to_db_names_dict,
    }
}