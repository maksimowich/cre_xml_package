import sqlalchemy


def execute_insert_query(engine: sqlalchemy.engine.base.Engine,
                         insert_query: str,
                         table_name: str,
                         number_of_rows: int):
    engine.execute(insert_query)
    print(str(number_of_rows) + ' rows were inserted into table ' + table_name)

