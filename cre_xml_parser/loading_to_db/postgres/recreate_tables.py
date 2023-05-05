import sqlalchemy
from sqlalchemy.orm import sessionmaker
from ...constants import TABLE_NAMES


def recreate_tables(prefix: str,
                    engine: sqlalchemy.engine.base.Engine):
    Session = sessionmaker(engine)
    with Session() as session:
        try:
            for table_name in TABLE_NAMES:
                drop_query = f"DROP TABLE IF EXISTS {prefix}{table_name}"
                session.execute(drop_query)
                create_query = f"CREATE TABLE IF NOT EXISTS {prefix}{table_name} AS " \
                               f"SELECT * FROM adm.sf_{table_name} WHERE 1<>1"
                session.execute(create_query)
        except Exception as e:
            session.rollback()
            raise e
        else:
            session.commit()
