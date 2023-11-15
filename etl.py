import psycopg2

from conn import create_edu

conn_edu = create_edu(psycopg2)
conn_edu.autocommit = False
cursor_edu = conn_edu.cursor()


class Mixin:
    def get_table_name(self):
        return self.__dict__.pop('_table_name')

    def get_keys(self):
        return self.__dict__.pop('_keys_list')

    def get_columns(self):
        return self.__dict__.copy()


class ExistingSourceTable(Mixin):
    @staticmethod
    def get_new_columns(cursor, table_name):
        """
        Вернет список столбцов: "table_name"
        """
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(query)
        return [x[0] for x in cursor_edu.fetchall()]

    @staticmethod
    def get_new_keys(cursor, table_name):
        """
        Вернет список ключей: "table_name"
        """
        query = f"""SELECT c.column_name
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                      AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                    WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';"""
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

    def __init__(self, cursor=None, table_name='', keys_list=()):
        self._table_name = table_name
        self._keys_list = (*self.get_new_keys(cursor, self._table_name),) if not keys_list else keys_list
        self.__dict__.update([(x, x) for x in self.get_new_columns(cursor, self._table_name)])


class NonExistingSourceTable(Mixin):
    def __init__(self, table_name='', keys_list=(), columns_list=()):
        self._table_name = table_name
        self._keys_list = keys_list
        self.__dict__.update([(x, x) for x in columns_list])


class ETL:
    def __init__(self, source=None, stg=None, tgt=None):
        self.source_name = source.get_table_name()
        self.source_keys_list = source.get_keys()
        self.source_columns_list = source.get_columns()
        self.stg_name = stg.get_table_name()
        self.stg_keys_list = stg.get_keys()
        self.stg_columns_list = stg.get_columns()
        self.tgt_name = tgt.get_table_name()
        self.tgt_keys_list = tgt.get_keys()
        self.tgt_columns_list = tgt.get_columns()



a = ExistingSourceTable(cursor=cursor_edu, table_name='zhii_test')
b = NonExistingSourceTable(table_name='zhii_test',
                           columns_list={'schema_name': 'schema_name', 'table_name': 'table_name',
                                         'max_update_dt': 'max_update_dt'}, keys_list=('id', 2))



c = ETL(a)
print(c.__dict__)

conn_edu.commit()
cursor_edu.close()
conn_edu.close()
