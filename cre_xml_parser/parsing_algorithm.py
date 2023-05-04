import datetime
import xml.etree.ElementTree as ET
from typing import Callable
from .constants import TAGS, FIELD_NAMES_TO_EXCLUDE, TABLES_HJID, get_table_name_by_tag
from .loading_to_db.loading_types import LOADING_TYPE_TO_ADD_ROW_FUNCTION
from .loading_to_db.loading_types import LOADING_TYPE_TO_GET_ROW_FROM_FIELD_NAMES_AND_FIELD_VALUES_FUNCTION


def get_field_value(expected_type: str,
                    field_name: str,
                    str_value: str):
    if expected_type is None:
        return None
    elif expected_type == 'int' or field_name == 'recentlegalupdatedate':  # костыль на interestrate пока в БД поле int а не float
        return int(str_value)
    elif field_name == 'interestrate':
        return int(str_value.split('.')[0])
    elif expected_type == 'float' or expected_type == 'moneyvaluetype':
        return float(str_value)
    else:
        return str_value


def get_row_from_sf_item(sf_item: ET.Element,
                         table_name: str,
                         hjid: int,
                         tables_current_hjid: dict,
                         get_row_from_field_names_and_filed_values: Callable,
                         tag_to_row_names_to_db_names_dict: dict):
    field_values = []
    field_names = []
    for sf_subitem in sf_item:
        field_name = sf_subitem.tag.lower().replace('_', '')
        if len(sf_subitem) > 0 \
                or field_name in FIELD_NAMES_TO_EXCLUDE \
                or tag_to_row_names_to_db_names_dict[sf_item.tag].get(field_name) is None:
            continue
        field_value = get_field_value(expected_type=tag_to_row_names_to_db_names_dict[sf_item.tag].get(field_name),
                                      field_name=field_name,
                                      str_value=sf_subitem.text)
        if field_value is not None:
            field_names.append(field_name)
            if isinstance(field_value, int) or isinstance(field_value, float):
                field_values.append(field_value)
            else:
                field_values.append("'" + field_value + "'")

    field_names.append('hdp_datetime')
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    field_values.append("'" + current_time + "'")

    if TABLES_HJID[table_name] != 'hjid':
        field_for_hjid_name = table_name + "_hjid"
        tables_current_hjid[field_for_hjid_name] = tables_current_hjid.get(field_for_hjid_name, 0) + 1
        field_names.append('hjid')
        field_values.append(tables_current_hjid.get(field_for_hjid_name))

    if table_name != 'monthlydetailtype':
        field_names.append(TABLES_HJID[table_name])
        field_values.append(hjid)
    else:
        field_names.append('loan_id')
        field_values.append(tables_current_hjid.get('loanstype_hjid'))
    return get_row_from_field_names_and_filed_values(field_names, field_values)


def parse_tag_in_xml_file(xml_root: ET.Element,
                          hjid: int,
                          table_name_to_rows_dict: dict,
                          tables_current_hjid: dict,
                          tag: str,
                          add_row: Callable,
                          get_row_from_field_names_and_filed_values: Callable,
                          tag_to_row_names_to_db_names_dict: dict):  # процедура парсинга тега в XML
    table_name = get_table_name_by_tag(tag)
    sf_items = xml_root.findall('.//' + tag)
    for sf_item in sf_items:
        row_to_append = get_row_from_sf_item(sf_item, table_name, hjid, tables_current_hjid,
                                             get_row_from_field_names_and_filed_values,
                                             tag_to_row_names_to_db_names_dict)
        add_row(table_name_to_rows_dict, table_name, row_to_append)
        if tag == 'LOAN':
            md_sf_items = sf_item.findall(".//MONTHLY_DETAIL")
            for md_sf_item in md_sf_items:
                md_table_name = get_table_name_by_tag(md_sf_item.tag)
                row_to_append = get_row_from_sf_item(md_sf_item, md_table_name, hjid, tables_current_hjid,
                                                     get_row_from_field_names_and_filed_values,
                                                     tag_to_row_names_to_db_names_dict)
                add_row(table_name_to_rows_dict, md_table_name, row_to_append)


def parse_xml_file(path_to_xml_file: str,
                   hjid: int,
                   table_name_to_df_dict: dict,
                   tables_current_hjid: dict,
                   loading_type: str,
                   tag_to_row_names_to_db_names_dict: dict):
    add_row_function = LOADING_TYPE_TO_ADD_ROW_FUNCTION[loading_type]
    get_row_from_field_names_and_filed_values_function = \
        LOADING_TYPE_TO_GET_ROW_FROM_FIELD_NAMES_AND_FIELD_VALUES_FUNCTION[loading_type]
    xml_root = ET.parse(path_to_xml_file).getroot()
    for tag in TAGS:  # парсим данные по тегу и накапливаем во фрейм
        if tag == 'MONTHLY_DETAIL':
            continue
        parse_tag_in_xml_file(xml_root,
                              hjid,
                              table_name_to_df_dict,
                              tables_current_hjid,
                              tag,
                              add_row_function,
                              get_row_from_field_names_and_filed_values_function,
                              tag_to_row_names_to_db_names_dict)  # парсим тег в файле
