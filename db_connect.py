import sqlite3
from typing import Optional, Sequence, Generator


class Db:
    """класс работы с базой sqllite3
    в виде with db_connect.Db(":memory:") as db: """
    def __init__(self, db):
        self.con = sqlite3.connect(db)
        self.con.row_factory = sqlite3.Row
        self.generator_cursor = self.con.cursor()

    def __enter__(self):
        """make a database connection"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """make sure the dbconnection gets closed"""
        self.con.close()

    def fetchall(self, query: str, **params) -> list:
        """Fetches all (remaining) rows of a query result
           Args:
                query: запрос
                params: параметры запроса
           Returns:
                список кортежей данных
        """
        cur = self.con.cursor()
        cur.execute(query, params)
        query_result = cur.fetchall()
        return [tuple(x) for x in query_result] if query_result else []

    def fetchall_as_dict(self, query: str, **params) -> list:
        """Fetches all (remaining) rows of a query result
        Args:
           query: запрос
           params: параметры запроса
        Returns:
            список словарей данных
        """
        cur = self.con.cursor()
        cur.execute(query, params)
        query_result = cur.fetchall()
        return [dict(x) for x in query_result] if query_result else []

    def fetchone(self, query, **params) -> tuple:
        """возвращает одну(следующую строку)
        Args:
           query: запрос
           params: параметры запроса
        Returns:
            кортеж данных
        """
        cur = self.con.cursor()
        cur.execute(query, params)
        query_result = cur.fetchone()
        return tuple(query_result) if query_result else ()

    def fetchmany(self, query, size=None, **kwargs) -> Generator:
        """ fetch next set as tuple
            Args:
                query: запрос
                size: размер выборки за раз
            Returns:
                список кортежей данных
        """
        self.generator_cursor.execute(query, kwargs)
        while True:
            results = self.generator_cursor.fetchmany(size=self.generator_cursor.arraysize)
            if not results:
                print('no results returned/end')
                break
            yield [tuple(x) for x in results]

    def fetchmany_as_dict(self, query, size=None, **kwargs) -> Generator:
        """ fetch next set as dict
            Args:
                query: запрос
                size: размер выборки за раз
            Returns:
                список словарей данных
        """
        self.generator_cursor.execute(query, kwargs)
        while True:
            results = self.generator_cursor.fetchmany(size=self.generator_cursor.arraysize)
            if not results:
                break
            yield [dict(x) for x in results]

    def fetchone_as_dict(self, query, **params) -> dict:
        """возвращает одну(следующую строку) в виде словаря
        Args:
           query: запрос
        Returns:
            словарь данных
        """
        cur = self.con.cursor()
        cur.execute(query, params)
        query_result = cur.fetchone()
        return dict(query_result) if query_result else {}

    def execute(self, query, **params) -> Optional[int]:
        """выполнение запроса
        Args:
           query: запрос
        Returns:
             число обработанных строк/None
        Raises:
            sqlite3.Error
        """
        cur = self.con.cursor()
        try:
            cur.execute(query, params)
            self.con.commit()
            return cur.rowcount
        except sqlite3.Error:
            self.con.rollback()

    def executemany(self, query, data: Sequence) -> Optional[int]:
        """вставка массива
        Args:
           query: запрос
           data: массив данных для всавки
        Returns:
             число вставленных строк/None
        Raises:
            sqlite3.Error
        """
        cur = self.con.cursor()
        try:
            cur.executemany(query, data)
            self.con.commit()
            return cur.rowcount
        except sqlite3.Error:
            self.con.rollback()

    def executescript(self, script: str) -> None:
        """выполнение скрипта
        Args:
            script: выполняемый скрипт
        Returns:
            None
        Raises:
            sqlite3.Error
            """
        cur = self.con.cursor()
        try:
            cur.executescript(script)
            self.con.commit()
        except sqlite3.Error:
            self.con.rollback()
