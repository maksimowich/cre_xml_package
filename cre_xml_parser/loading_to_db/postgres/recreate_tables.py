import sqlalchemy
from ...constants import TABLE_NAMES


def recreate_tables(prefix: str,
                    engine: sqlalchemy.engine.base.Engine):
    for table_name in TABLE_NAMES:
        drop_query = f"DROP TABLE IF EXISTS {prefix}{table_name}"
        engine.execute(drop_query, engine)
        create_query = f"CREATE TABLE IF NOT EXISTS {prefix}{table_name} AS " \
                       f"SELECT * FROM adm.sf_{table_name} WHERE 1<>1"
        engine.execute(create_query, engine)
