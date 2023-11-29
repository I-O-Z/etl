def add_log(func):
    def wrapper(*args, **kwargs):
        res = {'# The result of the ': f'{func.__name__}() function.'}
        res.update(func(*args, **kwargs))
        return res

    return wrapper


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


class ExistingSTG(ExistingTable):
    def __init__(self, cursor, table_name, tech_columns=('create_dt', 'update_dt'), keys_list=()):
        super().__init__(cursor, table_name, tech_columns)
        self.meta.keys = keys_list


class ExistingTGT(ExistingTable):
    def __init__(self, cursor, table_name,
                 tech_columns=('effective_from', 'effective_to', 'deleted_flg'), keys_list=()):
        super().__init__(cursor, table_name, tech_columns)
        self.meta.keys = keys_list


class ETL:
    @staticmethod
    def get_difference_of_dict(first, second):
        return {k: v for (k, v) in first.items() if k not in second}

    @staticmethod
    def get_source_keys(keys, dict_columns):
        """
        Вернет ключи соответствующие source
        """
        return [x for x, y in dict_columns.items() if y in keys]

    def __init__(self, source=None, stg=None, tgt=None, delete_table='', meta_table=''):
        self.delete_table = delete_table
        self.meta_table_name = meta_table
        self.meta_all_columns = tgt.get_new_columns(tgt.meta.cursor, self.meta_table_name)

        self.source_meta = source.get_meta() if source else source
        self.source_all_columns = source.get_all_columns() if source else source
        self.source_columns = self.get_difference_of_dict(self.source_all_columns, self.source_meta.tech_columns)
        self.changing_source_columns = self.get_difference_of_dict(self.source_columns, self.source_meta.keys)

        self.stg_meta = stg.get_meta() if stg else stg
        self.stg_all_columns = stg.get_all_columns() if stg else stg
        self.stg_columns = self.get_difference_of_dict(self.stg_all_columns, self.stg_meta.tech_columns)
        self.stg_meta.keys = self.get_source_keys(
            self.source_meta.keys, self.stg_all_columns) if not self.stg_meta.keys else self.stg_meta.keys
        self.changing_stg_columns = self.get_difference_of_dict(self.stg_columns, self.stg_meta.keys)

        self.tgt_meta = tgt.get_meta() if tgt else tgt
        self.tgt_all_columns = tgt.get_all_columns() if tgt else tgt
        self.tgt_columns = self.get_difference_of_dict(self.tgt_all_columns, self.tgt_meta.tech_columns)
        self.tgt_meta.keys = self.get_source_keys(
            self.stg_meta.keys, self.tgt_all_columns) if not self.tgt_meta.keys else self.tgt_meta.keys
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
    def write_condition(first_name, first, second_name, second, flg, for_prefix=()):
        """
        возвращает строку с условием на неравнство переданых значений
        """
        cond = '1=0 '
        for x, y in zip(first, second):
            x = ETL.add_prefix(x, first_name, for_prefix)
            cond += f"""or (( {second_name}.{y} <> {x} )
            or ( {second_name}.{y} is null and {x} is not null)
            or ( tgt.{y} is not null and {x} is null) )\n            """
        return cond + f" or tgt.{flg} = 'Y'"

    @add_log
    def clear_stg_tables(self):
        """
        Очистка stg таблиц
        """
        return {'cursor_dwh.execute': f"delete from {self.stg_meta.table_name};\ndelete from {self.delete_table};"}

    @add_log
    def get_source_date(self):
        """
        Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        """
        return {'cursor_dwh.execute': (
            f"""select cast(max_update_dt as varchar) 
                from {self.meta_table_name} where table_name = '{self.tgt_meta.table_name}'"""),
            'meta_dt': f"str({'cursor_dwh'}.fetchone()[0])",
            'cursor_source.execute': f""" Select {', '.join(self.stg_all_columns.values())}            
                from {self.source_meta.table_name}
                    where cast(coalesce(update_dt, create_dt)as timestamp) > cast(""" + "'{meta_dt}' as timestamp);",
            'cursor_dwh.executemany': (
                f"""INSERT INTO {self.stg_meta.table_name}(\n{', '.join(self.stg_all_columns.keys())})
            VALUES( {'%s' + ', %s' * (len(self.stg_all_columns) - 1)})""", 'cursor_dwh.fetchall())')}

    @add_log
    def get_keys_to_del(self):
        """
        Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
        """
        query = {'cursor_source.execute': f""" 
        Select {', '.join(self.source_meta.keys)} from {self.source_meta.table_name};""",
                 'cursor_dwh.executemany': (f"""INSERT INTO {self.delete_table} ({', '.join(self.source_meta.keys)})
                    VALUES({'%s' + ', %s' * (len(self.source_meta.keys) - 1)});""", 'cursor_dwh.fetchall())')}
        return query

    @add_log
    def load_inserts(self):
        """
        Загрузка в приемник "вставок" на источнике (формат SCD2)
        """
        return {'cursor_dwh.execute': f"""insert into {self.tgt_meta.table_name} (\n {', '.join(self.tgt_all_columns)} )
             select
                {', '.join([self.add_prefix(x, 'stg',
                                            self.source_all_columns.keys()) for x in (self.tgt_columns.values())])},
                coalesce({', stg.'.join(self.stg_meta.tech_columns)}) as {self.tgt_meta.tech_columns[0]},
                to_date('9999-12-31','YYYY-MM-DD') as {self.tgt_meta.tech_columns[1]},
                'N' as {self.tgt_meta.tech_columns[2]}
                    from {self.stg_meta.table_name} stg
                     left join {self.tgt_meta.table_name} tgt
                     on {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
                    where tgt.{self.tgt_meta.keys[0]} is null;"""}

    @staticmethod
    def use_crutch(a):
        """
        это 'костыль', он скоро исчезнет)
        """
        res = []
        for i in a:
            s = ''
            for j in i:
                if j == '(':
                    break
                else:
                    s += j
            res.append(s)
        return res

    @add_log
    def load_updates(self):
        """
        Обновление в приемнике "обновлений" на источнике (формат SCD2).
        """
        return {'cursor_dwh.execute': f"""insert into {self.tgt_meta.table_name} 
                ( {', '.join(self.tgt_all_columns)} )
            select 
                {', '.join([self.add_prefix(x, 'stg',
                                            self.stg_columns.values()) for x in (self.tgt_columns.values())])},
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
                                        self.tgt_meta.tech_columns[2],
                                        for_prefix=self.changing_stg_columns.values())};""",
                'cursor_dwh.execute_2': f"""update {self.tgt_meta.table_name} tgt
           set {self.tgt_meta.tech_columns[1]} = tmp.update_dt - interval '1 second'
        from (
            select
                {', '.join([self.add_prefix(x, 'stg',
                                            self.stg_columns.values()) for x in (self.tgt_columns.values())])},
                coalesce(stg.update_dt, stg.create_dt) as update_dt
            from {self.stg_meta.table_name} stg inner join
                 {self.tgt_meta.table_name} tgt
              on {self.compare_keys('stg', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
             and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
            where {self.write_condition('stg', self.changing_tgt_columns.values(),
                                        'tgt', self.changing_tgt_columns.keys(),
                                        self.tgt_meta.tech_columns[2],
                                        for_prefix=self.changing_stg_columns.values())}) tmp
        where {self.compare_keys('tmp', self.stg_meta.keys, 'tgt', self.tgt_meta.keys)}
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
          and ({self.write_condition('stg', self.use_crutch(self.changing_tgt_columns.values()),
                                     'tgt', self.changing_tgt_columns.keys(),
                                     self.tgt_meta.tech_columns[2],
                                     for_prefix=self.changing_stg_columns.values()).replace('stg',
                                                                                            'tmp')[6::]});"""}

    @add_log
    def process_deletions(self):
        """
        Обработка удалений в приемнике (формат SCD2).
        """
        return {'cursor_dwh.execute': f"""insert into {self.tgt_meta.table_name} 
                                ( {', '.join(self.tgt_all_columns)} )
                                select
                                    tgt.{', tgt.'.join(self.tgt_columns.keys())},
                                    now() {self.tgt_meta.tech_columns[0]},
                                    to_date('9999-12-31','YYYY-MM-DD') {self.tgt_meta.tech_columns[1]},
                                    'Y' {self.tgt_meta.tech_columns[2]}
                                from {self.tgt_meta.table_name} tgt left join
                                     {self.delete_table} stg
                                  on {self.compare_keys('stg', self.source_meta.keys, 'tgt', self.tgt_meta.keys)}
                                where stg.{self.stg_meta.keys[0]} is null
                                  and tgt.{self.tgt_meta.tech_columns[1]} = to_date('9999-12-31','YYYY-MM-DD')
                                  and tgt.{self.tgt_meta.tech_columns[2]} = 'N';""",

                'cursor_dwh.execute_2': f"""update {self.tgt_meta.table_name} tgt
                                   set {self.tgt_meta.tech_columns[1]} = now() - interval '1 second'
                                where concat(tgt.{', tgt.'.join(self.tgt_meta.keys)}, ' ') in (
                                    select
                                        concat(tgt.{', tgt.'.join(self.tgt_meta.keys)}, ' ')
                                    from {self.tgt_meta.table_name} tgt left join
                                         {self.delete_table} stg
                                       on {self.compare_keys('stg', self.source_meta.keys, 'tgt', self.tgt_meta.keys)}
                                    where stg.{self.stg_meta.keys[0]} is null
                                      and tgt.{self.tgt_meta.tech_columns[1]} = to_date('9999-12-31','YYYY-MM-DD')
                                  and tgt.{self.tgt_meta.tech_columns[2]} = 'N')
                                  and tgt.{self.tgt_meta.tech_columns[1]} = to_date('9999-12-31','YYYY-MM-DD')
                                  and {self.tgt_meta.tech_columns[2]} = 'N';"""}

    @add_log
    def update_meta(self):
        """
        Обновляет метаданных для таблици 'stg_name'.
        """
        print(self.stg_meta.tech_columns[::-1])
        return {'cursor_dwh.execute': f"""insert into {self.meta_table_name}( {', '.join(self.meta_all_columns)} )
                    select                      
                        '{self.stg_meta.table_name}', 
                        coalesce((select max(cast(coalesce({', '.join(self.stg_meta.tech_columns[::-1])})as timestamp)) 
                    from {self.stg_meta.table_name}), to_date('1900-01-01','YYYY-MM-DD'))
                    where not exists (select 1 from {self.meta_table_name} 
                                where table_name = '{self.stg_meta.table_name}');""",

                'cursor_dwh.execute_2': f"""update {self.meta_table_name}
                      set max_update_dt = coalesce((select max(cast(coalesce(update_dt, create_dt)as timestamp)) 
                      from {self.stg_meta.table_name}), max_update_dt)
                    where table_name = '{self.tgt_meta.table_name}';"""}

    def get_query(self):
        """
        Поочередно вызывает все функции генерации SQL
        """
        return [self.clear_stg_tables(), self.get_source_date(), self.get_keys_to_del(), self.load_inserts(),
                self.load_updates(), self.process_deletions(), self.update_meta()]

    @staticmethod
    def create_py_script(queries):
        """
        Создаст в рабочем каталоге файл 'etl_py_script.py'(скрипт с ETL для обьявленных вами таблиц.)
        """
        with open('etl_py_script.py', 'w') as file:
            file.write('import psycopg2\n\n')
            file.write('# create connections and disable the autocommit!\n\n')
            file.write("cursor_source = 'Declare the source cursor!!!'\n")
            file.write("cursor_dwh = 'Declare the storage cursor!!!'\n\n")

            for query in queries:
                for x, y in query.items():
                    if x[0] == '#':
                        file.write(f"{x}{y}\n")
                    else:
                        if x in ('cursor_dwh.execute_2', 'cursor_dwh.execute'):
                            file.write('cursor_dwh.execute')
                        else:
                            file.write(f"{x}")
                        if x == 'meta_dt':
                            file.write(f" = {y}\n")
                        elif type(y) == str:
                            file.write(f"(f\"\"\"{y}\"\"\")\n\n")
                        else:
                            file.write(f"(\"\"\"{y[0]}\"\"\"")
                            file.write(f", {y[1]}\n\n")
            file.write('\n# close the connections!')
