import sqlalchemy
from sqlalchemy.orm import sessionmaker


def execute_insert_query(engine: sqlalchemy.engine.base.Engine,
                         insert_query: str,
                         table_name: str,
                         number_of_rows: int):
    Session = sessionmaker(engine)
    with Session() as session:
        try:
            session.execute(insert_query)
        except:
            session.rollback()
            print(f'Something went wrong while inserting {number_of_rows} to {table_name}. Rollback.')
        else:
            session.commit()
            print(f'{str(number_of_rows)} rows were inserted into table {table_name}')
