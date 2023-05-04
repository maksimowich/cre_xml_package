import sqlalchemy
import threading
from .execute_insert_query import execute_insert_query


def save_singleformattype_to_db(hjids: list,
                                prefix: str,
                                engine: sqlalchemy.engine.base.Engine,
                                threads: list):
    if len(hjids) > 0:
        values_list = []
        for hj in hjids:
            values_list.append("(" + ",".join([str(hj)] * 8) + ")")
        insert_query = "INSERT INTO {}singleformattype" \
                       "(hjid, names_, loansoverview, loans, frauds, documents, scores, main)" \
                       "VALUES {};".format(prefix, ",".join(values_list))
        thread = threading.Thread(target=execute_insert_query,
                                  args=(engine, insert_query, prefix + 'singleformattype', len(values_list)))
        thread.start()
        threads.append(thread)
