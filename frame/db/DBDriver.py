import numpy as np
import pandas as pd

from frame.base.CompBase import *

class DBDriver(CompBase):
    def __init__(self, child_comp_name, log_level=CompBase.LOG_DEBUG):
        CompBase.__init__(self, child_comp_name, log_level=log_level)
        self._cur = None
        self._table_names = None
        self._table_name = None

    def set_table_name(self, table_name):
        self._table_name = table_name

    def get_table_count(self):
        return len(self._table_names)

    def _gen_column_list(self, column_list):
        # print type(column_list)
        if type(column_list) == str:
            if column_list == '*':
                column_list = self.get_column_list()
                self.d(str(column_list))
            else:
                return column_list

        cnt = len(column_list)
        sql_str = ''
        for i, f in enumerate(column_list, 1):
            sql_str += f
            if i != cnt:
                sql_str += ', '
        return sql_str

    def get_table(self, column_list='*', condition='', f_row_form=False, dtype=None, dtype_list=None):

        def type_convertor(df):

            def type_convertor_int64(x):

                if isinstance(x, (int, long, float)):
                    return int(x)
                elif isinstance(x, str):
                    return int(float(x))

            def type_convertor_str_int(x):
                if type(x) == float or type(x) == int:
                    return str(int(x))
                elif type(x) == str:
                    return str(int(x))

            def type_convertor_boolean(x):
                if x in ['True', 1.0, True, 1, '1', '1'] :
                    return True
                elif x in ['False', 0.0, False, 0, '0', '0']:
                    return False
                else:
                    raise Exception('boolean is not String, type :{}, data : {}'.format(type(x), x))


            if dtype_list == None:
                return df
            else:

                dtypes = [ dtype_list[x] for x in column_list]

                for i, x in enumerate(column_list):
                    if dtypes[i] is None:
                        continue
                    else:
                        if dtypes[i] == 'pk':
                            df[x] = df[x].astype('int32')
                        elif dtypes[i] in ['bool', bool]:
                            df[x] = df[x].apply(type_convertor_boolean)
                        elif dtypes[i] == 'str int':
                            df[x] = df[x].apply(type_convertor_str_int)
                        elif dtypes[i] == 'int64':
                            df[x] = df[x].apply(type_convertor_int64)
                        else:
                            df[x] = df[x].astype(dtypes[i])
                return df

        if column_list == '*':
            column_list = self.get_column_list()
            # print column_list

        sql_str = 'select '
        sql_str += self._gen_column_list(column_list)

        if condition == '':
            sql_str += ' from {} '.format(self._table_name)
        else:
            sql_str += ' from {} where {}'.format(self._table_name, condition)
        # print sql_str
        if column_list == ' * ':
            column_list = self.get_column_list()

        row = self.get_sql(sql_str)
        if row is not None:
            cnt = len(row)
        else:
            self.w('sql data is emtpy')
            cnt = 0
        self.d('Count : {} at table {}'.format(cnt, self._table_name))
        if f_row_form:
            return row

        column_list = [w.replace('`AS`', 'AS') for w in column_list]

        if cnt == 0:
            self.d('No Data')
            df = pd.DataFrame(columns=column_list)
            return pd.DataFrame(columns=column_list)
        if cnt == 1:
            return type_convertor(pd.DataFrame(np.array(list(row)), columns=column_list, dtype=dtype))

        df = pd.DataFrame(np.array(row), columns=column_list, dtype=dtype)
        return type_convertor(df)

    def get_column_list(self):
        column_list = [c.column_name for c in self._cur.columns(table=self._table_name)]
        # print column_list
        return column_list

    def get_row_count(self):
        self.d('accessing {}'.format(self._table_name))
        sql_str = 'select count(*) from {}'.format(self._table_name)
        self.d('SQL : {}'.format(sql_str))
        self._cur.execute(sql_str)
        row = self._cur.fetchall()
        self.d('row count : {}'.format(row[0][0]))
        return row[0][0]

    def get_group_count(self, group_name):
        row = self.get_sql('select {}, COUNT(*) from {} group by {}'.format(group_name, self._table_name, group_name))
        title = [group_name, 'count']
        return pd.DataFrame(np.array(row), columns=title)

    def get_max_row_at(self, column_list, condition):
        sql_str = 'Select '
        sql_str += self._gen_column_list(column_list)
        sql_str += ' from {} where {}=( select max({}) from {} )'.format(self._table_name, condition, condition,
                                                                         self._table_name)
        row = self.get_sql(sql_str)
        return row

    def get_sql(self, sql_str):
        self.d('SQL : {}'.format(sql_str))
        if self._cur is None:
            return
        self._cur.execute(sql_str)

        row = self._cur.fetchall()
        return row

    def _get_table_list(self):
        return None

    def create_table(self, field_list, table_name, col_num_index_list=None, f_all_num_col=False,
                     col_integer_index_list=None):
        table_list = self._get_table_list()
        if table_name in table_list:
            self.w('Table already exist')
            return

        sql_str = 'Create table {}('.format(table_name)
        cnt = len(field_list)
        for i, f in enumerate(field_list):

            if f_all_num_col or (col_num_index_list is not None and i in col_num_index_list):
                sql_str += '{} number'.format(f)
            elif col_integer_index_list is not None and i in col_integer_index_list:
                sql_str += '{} integer'.format(f)
            else:
                sql_str += '{} varchar'.format(f)
            if i != cnt - 1:
                sql_str += ', '
        sql_str += ')'
        self.commit(sql_str)

    def create_table_pk(self, field_list, table_name, data_type, data_property):

        data_type_table = {'int32': '{} INTEGER',
                           'float64': '{} number',
                           'str': '{} varchar',
                           'pk': '{} INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE'}

        # check table name
        table_list = self._get_table_list()
        if table_name in table_list:
            self.w('Table already exist')
            return

        # sql
        sql_str = 'Create table {}('.format(table_name)
        for f, d, p in zip(field_list, data_type, data_property):
            sql_str += data_type_table[d].format(f)
            sql_str += ','

        sql_str = sql_str[:-1]
        sql_str += ')'

        # commit
        self.commit(sql_str)

    def insert_data_jk(self, field_list, data, table_name, col_type, col_property):

        data_type_table = {'int32': '{:d}',
                           'float64': '{}',
                           'str': '\'{}\'',
                           'pk': '{:d}'}

        field_list = field_list[1:]
        data = data[1:]
        col_type = col_type[1:]
        col_property = col_property[1:]

        sql_str = 'INSERT INTO {} ('.format(table_name)

        for f in field_list:
            sql_str += '{},'.format(f)
        sql_str = sql_str[:-1]
        sql_str += ') VALUES ('

        for adata, atype in zip(data, col_type):
            sql_str += data_type_table[atype].format(adata)
            sql_str += ','

        sql_str = sql_str[:-1]
        sql_str += ');'

        print(sql_str)

        self.commit(sql_str)

    def save_data_jk(self, field_list, data, table_name, col_type, col_property):

        pk = data[0]
        print(pk, type(pk))

        if type(pk) is int or type(pk) is np.int32:
            self.update_data_jk(field_list, data, table_name, col_type, col_property)
        elif pk is None:
            self.insert_data_jk(field_list, data, table_name, col_type, col_property)
        else:
            self.e("pk is not None, not Int")
            raise Exception

    def update_data_jk(self, field_list, data, table_name, col_type, col_property):

        data_type_table = {'int32': '{:d}',
                           'float64': '{}',
                           'str': "'{}'",
                           'pk': '{:d}'}

        pk = data[0]
        if type(pk) is not int and type(pk) is not np.int32:
            self.e('database update with out pk')
            raise Exception

        field_list = field_list[1:]
        data = data[1:]
        col_type = col_type[1:]
        col_property = col_property[1:]

        sql_str = "UPDATE {} SET".format(table_name)

        for f, d, c in zip(field_list, data, col_type):
            sql_str += " {} = ".format(f) + data_type_table[c].format(d)
            sql_str += ","
        sql_str = sql_str[:-1]
        sql_str += " WHERE "

        sql_str += "pk = {:d};".format(pk)

        self.commit(sql_str)

    def insert_data(self, field_list, data, table_name=None, col_num_index_list=None, f_all_num_col=False,
                    col_integer_index_list=None):
        if table_name is None:
            sql_str = 'INSERT INTO {} ('.format(self._table_name)
        else:
            sql_str = 'INSERT INTO {} ('.format(table_name)
        cnt = len(field_list)
        for i, f in enumerate(field_list, 1):
            sql_str += '{}'.format(f)
            if i != cnt:
                sql_str += ', '
        sql_str += ') VALUES ('
        cnt = len(data)
        for i, d in enumerate(data):
            f = field_list[i - 1]
            if f_all_num_col or (col_num_index_list is not None and i in col_num_index_list):
                sql_str += "{}".format(d)
            elif col_integer_index_list is not None and i in col_integer_index_list:
                sql_str += "{:d}".format(int(float(d)))
            else:
                sql_str += "'{}'".format(d)
            if i != cnt - 1:
                sql_str += ', '
        sql_str += ');'
        self.commit(sql_str)

    # values [ [],[] ]
    def insert_into_values(self, table, values, columns=None, colum_types=None, where=None):
        sql = 'insert into {}'.format(table)
        if columns:
            sql += " ({}) ".format(", ".join(columns))

        sql += ' values '

        if isinstance(values[0], list) is False:
            temp = []
            temp.append(values)
            values = temp

        row_count = len(values)
        for row_index, row in enumerate(values):
            count = len(row)
            sql += '('
            for index, (type, value) in enumerate(zip(colum_types, row)):
                if value is None:
                    if type == 'int32':
                        value = 0

                if type == 'str':
                    sql += '\'{}\''.format(value)
                else:
                    sql += '{}'.format(value)

                if index is not count - 1:
                    sql += ','
            sql += ')'

            if row_index is not row_count - 1:
                sql += ','

        self.commit(sql)
        # self._cur.execute(sql)

    def delete_data(self, table_name, conditon):
        sql_str = 'DELETE FROM {} WHERE {};'.format(table_name, conditon)
        self.commit(sql_str)

    def make_in_condition_from_list(self, data_list):
        list_str = '('
        for i, data in enumerate(data_list):
            if i == 0:
                list_str += '%d' % (data)
            else:
                list_str += ', %d' % (data)
        list_str += ')'
        return list_str

    def update_data(self, table_name, set_data, condition_data=None):
        if table_name is None:
            sql_str = 'UPDATE {} SET '.format(self._table_name)
        else:
            sql_str = 'UPDATE {} SET '.format(table_name)
        cnt = len(list(set_data.keys()))
        for i, key in enumerate(set_data.keys()):
            sql_str += '{} = '.format(key)
            value = set_data[key]
            # print key, value, type(value)
            if type(value) is str:
                sql_str += "'{}' ".format(value)
            else:
                sql_str += "{} ".format(value)
            if i != cnt - 1:
                sql_str += ', '
        if condition_data is not None:
            sql_str += 'WHERE '
            cnt = len(list(condition_data.keys()))
            for i, key in enumerate(condition_data.keys()):
                sql_str += '{} = '.format(key)
                value = condition_data[key]
                if type(value) is str:
                    sql_str += "'{}'".format(value)
                else:
                    sql_str += "{} ".format(value)
                if i != cnt - 1:
                    sql_str += ', '
        sql_str += ';'
        self.commit(sql_str)
