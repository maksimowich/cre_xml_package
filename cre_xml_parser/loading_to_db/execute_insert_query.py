import sqlalchemy


def execute_insert_query(engine: sqlalchemy.engine.base.Engine,
                         insert_query: str):
    engine.execute(insert_query)
