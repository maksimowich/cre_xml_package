import xml.etree.ElementTree as ET
from os.path import dirname


TAGS = [
    'MONTHLY_DETAIL',
    'LOANS_OVERVIEW',
    'LOAN',
    'MAIN',
    'NAME',
    'SCORE',
    'FRAUD',
]

TABLE_NAMES = [
    'singleformattype',
    'monthlydetailtype',
    'loansoverviewtype',
    'loanstype',
    'maintype',
    'nametype',
    'scoretype',
    'fraudtype',
]

TAGS_TO_TABLE_NAMES_MAPPING = {
    'MONTHLY_DETAIL': 'monthlydetailtype',
    'LOANS_OVERVIEW': 'loansoverviewtype',
    'LOAN': 'loanstype',
    'MAIN': 'maintype',
    'NAME': 'nametype',
    'SCORE': 'scoretype',
    'FRAUD': 'fraudtype',
}

FIELD_NAMES_TO_EXCLUDE = ['cbtypecode', 'nextpmtprincipal']

TABLES_HJID = {
    'monthlydetailtype': 'loan_id',
    'loansoverviewtype': 'hjid',
    'loanstype': 'loanstypes_loan_hjid',
    'maintype': 'hjid',
    'nametype': 'nametypes_name__hjid',
    'scoretype': 'scoretypes_score_hjid',
    'fraudtype': 'fraudtypes_fraud_hjid',
}


def get_table_name_by_tag(tag: str):
    return TAGS_TO_TABLE_NAMES_MAPPING[tag]


def get_tag_to_table_types_dict():
    xml_root = ET.parse(dirname(__file__) + '/resources/SingleFormat.xsd').getroot()
    result_dict = {}
    for tag in TAGS:
        table_type = xml_root.findall(".//{http://www.w3.org/2001/XMLSchema}element[@name='" + tag + "']")[0].attrib['type']
        table_types_dict = {}
        for element in xml_root.findall(".//{http://www.w3.org/2001/XMLSchema}complexType[@name='" + table_type
                                        + "']/{http://www.w3.org/2001/XMLSchema}sequence/{http://www.w3.org/2001/XMLSchema}element"):
            element_name = element.attrib['name']
            element_type = element.attrib['type']
            if element_type[:3] == "xs:":
                element_type = element_type[3:]
            table_types_dict[element_name.lower().replace('_', '')] = element_type.lower()
        result_dict[tag] = table_types_dict
    return result_dict


TAG_TO_TABLE_TYPES_DICT = get_tag_to_table_types_dict()
