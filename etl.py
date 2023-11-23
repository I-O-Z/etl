import psycopg2

from conn import create_edu

conn_edu = create_edu(psycopg2)
conn_edu.autocommit = False
cursor_edu = conn_edu.cursor()


class ExistingTable:
    @staticmethod
    def get_new_columns(cursor, table_name):
        """
        Вернет список столбцов: "table_name"
        """
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

    def __init__(self, cursor, table_name, tech_columns=('create_dt', 'update_dt')):
        self.meta = self.Meta(cursor, table_name, tech_columns)
        self.__dict__.update([(x, x) for x in self.get_new_columns(cursor, table_name)])

    class Meta:
        def __init__(self, cursor, table_name, tech_columns):
            self.cursor = cursor
            self.table_name = table_name
            self.tech_columns = tech_columns

    def get_meta(self):
        return self.__dict__.pop('meta')

    def get_all_columns(self):
        return self.__dict__.copy()


class SourceMixin:
    @staticmethod
    def get_new_keys(cursor, table_name, keys_list):
        """
        Вернет список ключей: "table_name"
        """
        if keys_list:
            return keys_list
        else:
            query = f"""
                SELECT c.column_name
                   FROM information_schema.table_constraints tc 
                   JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                   JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                     AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                   WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';"""
            cursor.execute(query)
            return tuple([x[0] for x in cursor.fetchall()])


class ExistingSource(SourceMixin, ExistingTable):

    def __init__(self, cursor, table_name, tech_columns=('create_dt', 'update_dt'), keys_list=()):
        super().__init__(cursor, table_name, tech_columns)
        self.meta.keys = self.get_new_keys(cursor, table_name, keys_list)


###################################################################################################
class ExistingSTG(ExistingTable):
    def __init__(self, cursor, table_name, tech_columns=('create_dt', 'update_dt'), keys_list=()):
        super().__init__(cursor, table_name, tech_columns)
        self.meta.keys = keys_list


###################################################################################################
class ExistingTGT(ExistingTable):
    def __init__(self, cursor, table_name,
                 tech_columns=('effective_from', 'effective_to', 'deleted_flg'), keys_list=()):
        super().__init__(cursor, table_name, tech_columns)
        self.meta.keys = keys_list


###########################################################################

class ETL:
    @staticmethod
    def get_difference_of_dict(first, second):
        return {k: v for (k, v) in first.items() if k not in second}

    def __init__(self, source=None, stg=None, tgt=None, delete_table=''):
        self.delete_table = delete_table

        self.source_meta = source.get_meta() if source else source
        self.source_all_columns = source.get_all_columns() if source else source

        self.stg_meta = stg.get_meta() if stg else stg
        self.stg_all_columns = stg.get_all_columns() if stg else stg
        self.stg_columns = self.get_difference_of_dict(self.stg_all_columns, self.stg_meta.tech_columns)
        self.changing_stg_columns = self.get_difference_of_dict(self.stg_columns, self.stg_meta.keys)

        self.tgt_meta = tgt.get_meta() if tgt else tgt
        self.tgt_all_columns = tgt.get_all_columns() if tgt else tgt
        self.tgt_columns = self.get_difference_of_dict(self.tgt_all_columns, self.tgt_meta.tech_columns)
        self.changing_tgt_columns = self.get_difference_of_dict(self.tgt_columns, self.tgt_meta.keys)

    @staticmethod
    def compare_keys(first_name, first, second_name, second):
        """
        возвращает строку с условием на равнство переданых значений
        """
        cond = '1=1 '
        for x, y in zip(first, second):
            cond += f'and ({first_name}.{x} = {second_name}.{y}) '
        return cond

    @staticmethod
    def add_prefix(query, pref, columns):
        """
        Вернет строку query, подставит pref перед столбцами из списка columns
        """
        str_res = ''
        res = []
        for i in query:
            if i not in (',', '(', ')', ' '):
                str_res += i
            else:
                res.append(str_res) if str_res not in columns else res.append(f'{pref}.{str_res}')
                res.append(i)
                str_res = ''
        return ''.join(res) if res else f'{pref}.{str_res}'

    @staticmethod
    def write_condition(first_name, first, second_name, second, flg):
        """
        возвращает строку с условием на неравнство переданых значений
        """
        cond = '1=0 '
        for x, y in zip(first, second):
            cond += f"""or ( {first_name}.{y} <> {second_name}.{x} 
            or ( {first_name}.{y} is null and {second_name}.{x} is not null) 
            or ( stg.{y} is not null and tgt.{x} is null) )"""
        return cond + f" or tgt.{flg} = 'Y'"

    def get_source_date(self):
        """
        Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        """
        print \
            (f""" Select {', '.join(self.stg_all_columns.values())}
                from {self.source_meta.table_name}
                    where cast(coalesce(update_dt, create_dt)as timestamp) > cast('{'!!!!meta_dt!!!'}' as timestamp)""")
        print(f"""INSERT INTO {self.stg_meta.table_name}(
                    {', '.join(self.stg_all_columns.keys())})
                    VALUES( {'%s' + ', %s' * (len(self.stg_all_columns) - 1)})""", 'cursor_edu.fetchall()')

    def get_keys_to_del(self):
        """
        Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
        """
        print(f""" Select {', '.join(self.source_meta.keys)} from {self.source_meta.table_name};""")
        print(f"""INSERT INTO {self.delete_table} ({', '.join(self.source_meta.keys)})
                    VALUES({'%s' + ', %s' * (len(self.source_meta.keys) - 1)})""", 'cursor_edu.fetchall()')

    def load_inserts(self):
        """
        Загрузка в приемник "вставок" на источнике (формат SCD2)
        """
        print(f"""insert into {self.tgt_meta.table_name} ( {', '.join(self.tgt_all_columns)} )
             select
                {', '.join([self.add_prefix(x, 'stg', self.source_all_columns.keys()) for x in (self.tgt_columns.values())])},
                coalesce({', stg.'.join(self.stg_meta.tech_columns)}) as {self.tgt_meta.tech_columns[0]},
                to_date('9999-12-31','YYYY-MM-DD') as {self.tgt_meta.tech_columns[1]},
                'N' as {self.tgt_meta.tech_columns[2]}
                    from {self.stg_meta.table_name} stg
                     left join {self.tgt_meta.table_name} tgt
                     on {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
                    where tgt.{self.tgt_meta.keys[0]} is null;""")

    def load_updates(self):
        """tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values,
                     tgt_columns_of_values, cursor_edu
        Обновление в приемнике "обновлений" на источнике (формат SCD2).
        """
        print(f"""insert into {self.tgt_meta.table_name} 
                ( {', '.join(self.tgt_all_columns)} )
            select 
                {', '.join([self.add_prefix(x, 'stg', self.stg_columns.values()) for x in (self.tgt_columns.values())])},
                stg.{self.stg_meta.tech_columns[0]} {self.tgt_meta.tech_columns[0]},
                to_date('9999-12-31','YYYY-MM-DD') {self.tgt_meta.tech_columns[1]},
                'N' {self.tgt_meta.tech_columns[2]}
            from {self.stg_meta.table_name} stg 
                    inner join 
                 {self.tgt_meta.table_name} tgt
                    on {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
                    and tgt.{self.tgt_meta.tech_columns[1]} = to_date('9999-12-31','YYYY-MM-DD')
            where {self.write_condition('stg', self.changing_tgt_columns.values(),
                                        'tgt', self.changing_tgt_columns.keys(),
                                        self.tgt_meta.tech_columns[2])};""")

        print(f"""update {self.tgt_meta.table_name} tgt
                   set {self.tgt_meta.tech_columns[1]} = tmp.update_dt - interval '1 second'
                from (
                    select
                        {', '.join([self.add_prefix(x, 'stg', self.stg_columns.values()) for x in (self.tgt_columns.values())])},
                        coalesce(stg.update_dt, stg.create_dt) as update_dt
                    from {self.stg_meta.table_name} stg inner join
                         {self.tgt_meta.table_name} tgt
                      on {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
                     and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                    where {self.write_condition('stg', self.changing_tgt_columns.values(),
                                                    'tgt', self.changing_tgt_columns.keys(),
                                                    self.tgt_meta.tech_columns[2])}) tmp
                where {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
                  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                  and ({self.write_condition('stg', self.changing_tgt_columns.values(),
                                                    'tgt', self.changing_tgt_columns.keys(),
                                                    self.tgt_meta.tech_columns[2]).replace('stg', 'tmp')[6::]});""")


######################################################################################
msource = ExistingSource(cursor_edu, 'zhii_source_test')
# print('source')
# print(msource.__dict__)
# print(msource.meta.__dict__)

mstg = ExistingSTG(cursor_edu, 'zhii_stg_test', keys_list=('id', 'passport'))

# print('stg')
# print(mstg.__dict__)
# print(mstg.meta.__dict__)

mtgt = ExistingTGT(cursor_edu, 'zhii_tgt_test', keys_list=('id', 'passport'))
mtgt.fio = f'concat({mstg.name}, {mstg.last_name}, {mstg.patronymic})'

# print('tgt')
# print(mtgt.__dict__)
# print(mtgt.meta.__dict__)
etl = ETL(source=msource, stg=mstg, tgt=mtgt, delete_table='zhii_test_del')

# etl.get_source_date()
# etl.get_keys_to_del()
etl.load_inserts()
etl.load_updates()

########################
conn_edu.commit()
cursor_edu.close()
conn_edu.close()
