import sys
from typing import Sequence
import psycopg2


class Database:
    def __init__(self,args:dict[str,str]):
        self.conn = self.connect(args)

    def connect(self,args:dict[str,str]):
        """
        Connect to database and return connection
        """
        try:
            conn = psycopg2.connect(
                    host = args["host"],
                    dbname = args["db"],
                    user = args["user"],
                    password = args["password"],
                    port = args["port"])
        except psycopg2.OperationalError as e:
            print(f"Could not connect to Database: {e}")
            sys.exit(1)

        return conn

    def cursor(self):
        return self.conn.cursor()
    def cursor_close(self):
        return self.conn.close()

    def commit(self):
        return self.conn.commit()
    def rollback(self):
        return self.conn.rollback()
    def execute_noparm(self, line: str):
        return self.cursor().execute(line)
    def execute(self, line: str, parms: Sequence):
        return self.cursor().execute( line, parms )
    def close(self):
        if self.conn is not None:
            self.conn.close()



class Db:
    def __init__(self,args:dict[str,str]):
        self.db = Database(args)
        self.cursor = None

    def __enter__(self):
        return self

    def query(self, query_str: str, parms: Sequence = None):
        with self.db.cursor() as curs:
            if parms is None:
                curs.execute(query_str)
            else:
                curs.execute(query_str, parms)
            return curs.fetchall()


    def insert(self, insert_str: str,
               with_get_id: bool = False,
               parms: Sequence = None) -> None | int:
        id_to_ret = 0
        with self.db.cursor() as curs:
            if parms is None:
                curs.execute(insert_str)
            else:
                curs.execute(insert_str, parms)
            if with_get_id:
                id_to_ret = curs.fetchone()[0]
        self.db.commit()
        if with_get_id:
            return id_to_ret


    def get_cursor(self):
        return self.db.cursor()

    def insert_continuous(self, insert_str: str,
                          with_get_id: bool = False,
                          parms: Sequence = None)-> None | int:
        id_to_ret = 0
        if self.cursor is None:
            self.cursor = self.get_cursor()
        if parms is None:
            self.cursor.execute(insert_str)
        else:
            self.cursor.execute(insert_str, parms)
        if with_get_id:
            id_to_ret = self.cursor.fetchone()[0]
        if with_get_id:
            return id_to_ret

    def close_cursor(self):
        if self.cursor is not None:
            self.cursor.close()
        self.cursor = None

    def commit(self)->None:
        self.close_cursor()
        self.db.commit()


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

