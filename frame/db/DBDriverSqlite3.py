import os
import sqlite3
from DBDriver import DBDriver
from frame.base.CompBase import CompBase


class DBDriverSqlite3(DBDriver):
    def __init__(self, file_path, table_name=None, password=None, log_level=CompBase.LOG_INFO, creatable=False):
        DBDriver.__init__(self, 'DrvSqlite', log_level=log_level)
        self.extension = 'db'
        self.title = 'Sqlite3'
        self.file_path = file_path
        self._cur = None
        self._table_name = table_name
        self.__con = None

        # file_path = '../../Data/PAMsqlite.db'

        f_created = False
        if not os.path.exists(file_path):
            self.w('Cannot find {}'.format(file_path))
            f_created = True
         # connect to db
        try:
            self.__con = sqlite3.connect(file_path)
            if f_created:
                self.i('Created DB file')
                if os.path.exists(file_path):
                    pass
        except:
            self.e('Fail to Open DB file')
            return

        self._cur = self.__con.cursor()

        if not f_created:
            # get table list
            self._table_names = self._get_table_list()

            if self._table_name is None and len(self._table_names) > 0:
                self._table_name = self._table_names[0]
                self.d("Table '{}' is selected".format(self._table_name))


        self.__con.text_factory = str

    def _get_table_list(self):
        self._cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_list = set()
        for row in self._cur.fetchall():
            table_list.add(str(row[0]))
        self.d('num. of table in db : {}'.format(len(table_list)))
        return list(table_list)

    def close(self):
        if self.__con:
            self.__con.close()
            self.__con = None
            self.d('Closed: {}'.format(self.file_path))

    def __del__(self):
        self.close()

    def commit(self, sql_str=[]):
        self.d('SQL : {}'.format(sql_str))
        try:
            if type(sql_str) not in [set, tuple, list]:
                sql_str = (sql_str,)
            for sql in sql_str:
                self._cur.execute(sql)
            self.__con.commit()
        except Exception as e:
            self.e('commit: {}'.format(e))
            self.__con.rollback()

    def commit_many(self):
        self.__con.commit()

# custom methods
    def get_column_list(self):
        self._cur = self.__con.execute('select * from {}'.format(self._table_name))
        return [member[0] for member in self._cur.description]