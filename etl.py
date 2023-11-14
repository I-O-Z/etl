import psycopg2

from conn import create_edu

conn_edu = create_edu(psycopg2)
conn_edu.autocommit = False
cursor_edu = conn_edu.cursor()


class SC:
    def get_columns(self, cursor, table_name):
        """
        Вернет список столбцов: "table_name"
        """
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(query)
        return [x[0] for x in cursor_edu.fetchall()]

    def __init__(self, cursor=None, table_name='', col_list=()):
        self.table_name = table_name
        self.__dict__.update([(x, x) for x in (col_list if col_list else self.get_columns(cursor, self.table_name))])


a = SC(cursor=cursor_edu, table_name='zhii_stg_accounts')
print(a.__dict__)
print(a.account)
conn_edu.commit()
cursor_edu.close()
conn_edu.close()
