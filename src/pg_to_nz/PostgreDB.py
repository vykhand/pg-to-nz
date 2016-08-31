import psycopg2 as pg
import logging, sys, os
import yaml

log = logging.getLogger()
cfg_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'config.yml')
cfg = yaml.load(open(cfg_file,'r'))

class PostgreDB:
    def __init__(self, database = None, user = None, password = None, host = None, port = None):
        self._db = database or cfg['pg_db']
        self._user = user or cfg['pg_user']
        self._pwd = password or cfg['pg_pwd']
        self._host = host or cfg['pg_host']
        self._port = port or cfg['pg_port']

    def test_connection(self):
        try:
            conn = self.connect()
            log.info("Connection successful")
            conn.close()
            return True
        except Exception, e:
            log.error("Connection could not be established: " + str(e))
            return False

    def connect(self):
        conn_string = "dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(self._db, self._user,
                                                                                       self._host, self._port, self._pwd)
        self._conn = pg.connect( conn_string)

        return self._conn

    def disconnect(self):
        self._conn.close()

    def get_table_list(self, schema = 'public', views = False):

        cond = "IN ('BASE TABLE', 'VIEW')" if views else "= 'BASE TABLE'"

        qry = """
        select table_name from information_schema.tables
        where table_schema = '{}' and table_type {} """.format(schema, cond)

        cur = self._conn.cursor()
        cur.execute(qry)
        tables = [a[0] for a in cur.fetchall()]
        return tables

    def get_mview_list(self, schema = 'public'):
        qry = """
        SELECT relname
        FROM   pg_class p JOIN pg_namespace s on p.relnamespace = s.oid
        WHERE  p.relkind = 'm' and s.nspname = '{}'
        """.format(schema)

        cur = self._conn.cursor()

        cur.execute(qry)
        mviews = [a[0] for a in cur.fetchall()]
        return mviews

    def get_mview_ddl(self, mview, schema = 'public'):
        qry = """
        SELECT  a.attname as col,
       pg_catalog.format_type(a.atttypid, a.atttypmod) as typ,
       a.attnotnull as is_null
        FROM pg_attribute a
          JOIN pg_class t on a.attrelid = t.oid
          JOIN pg_namespace s on t.relnamespace = s.oid
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND t.relname = '{}' --<< replace with the name of the MV
          AND s.nspname = '{}' --<< change to the schema your MV is in
        ORDER BY a.attnum
        """.format(mview, schema)

        cur = self._conn.cursor()

        cur.execute(qry)
        ddl = []
        ddl.append('CREATE TABLE {}  ('.format(mview))

        for col, data_type, is_null in cur.fetchall():

            if data_type.lower().find('character') == 0:
               if data_type.lower().find('(') > -1:
                    type_str = 'national {}'.format(data_type)
               # if just 'character varying'
               else: type_str = 'national {}(128)'.format(data_type)
            elif data_type == 'text':
                type_str = 'national character varying (3000)'
            elif data_type.lower().find('timestamp') == 0:
                type_str = 'timestamp'
            elif data_type == 'bytea':
                type_str = 'varchar(100)'
            elif data_type == 'numeric':
                type_str = 'float'
            else:
                # data_type in [,
                #             'smallint','integer', 'date', 'double precision', 'boolean' ]:
                type_str = data_type
            ddl.append('"{}" {},'.format(col, type_str))
        # getting rid of last comma
        ddl[-1] = ddl[-1].rstrip(',') + ')'

        # choosing random distribution key if first column name does not contain id
        # might be not a correct logic for all cases
        if "id" not in ddl[1].lower():
            ddl.append('DISTRIBUTE ON RANDOM')

        cur.close()
        return '\n'.join(ddl)

    def get_table_ddl(self, table, schema ='public'):


        qry = '''
        SELECT column_name, data_type, character_maximum_length,
          character_octet_length, numeric_precision, numeric_precision_radix,
         numeric_scale, datetime_precision, interval_type, interval_precision
         FROM information_schema.columns
        WHERE table_schema = '{}'
        AND table_name   = '{}'
        order by ordinal_position
        '''.format(schema, table)
        cur = self._conn.cursor()

        cur.execute(qry)
        ddl = []
        ddl.append( 'CREATE TABLE {}  ('.format(table))

        for column_name, data_type, character_maximum_length, \
            character_octet_length, numeric_precision, numeric_precision_radix, \
            numeric_scale, datetime_precision, interval_type, interval_precision in cur.fetchall():

            if data_type in ['character varying', 'character']:
                type_str = 'national {}({})'.format(data_type, character_maximum_length or 256)
            elif data_type == 'numeric':
                if numeric_precision is None or numeric_scale is None:
                    type_str = 'float'
                else:
                    type_str = '{}({},{})'.format(data_type, numeric_precision, numeric_scale)
            elif data_type == 'text':
                type_str = 'national character varying (3000)'
            elif data_type in ['timestamp with time zone', 'timestamp without time zone']:
                type_str = 'timestamp'
            elif data_type == 'bytea':
                type_str = 'varchar(100)'
            else:
                # data_type in [,
                #             'smallint','integer', 'date', 'double precision', 'boolean' ]:
                type_str = data_type
            ddl.append('"{}" {},'.format(column_name, type_str))
        #getting rid of last comma
        ddl[-1] = ddl[-1].rstrip(',') + ')'

        #choosing random distribution key if first column name does not contain id
        #might be not a correct logic for all cases
        if "id" not in ddl[1].lower():
            ddl.append('DISTRIBUTE ON RANDOM')

        cur.close()
        return '\n'.join(ddl)

    def get_db_ddl(self, schema = 'public', drop_table = True, lower = True, views = False):
        ddls = []
        for t in self.get_table_list(schema, views):
            if drop_table:
                ddls.append('DROP TABLE {};'.format(t))
                if lower:
                    ddls.append('DROP TABLE {};'.format('"' + t.lower() + '"'))
            ddls.append(self.get_table_ddl(t, schema))
            ddls.append('')
        return ddls

    def get_all_mview_ddl(self, schema = 'public', drop_table = True, lower = True):
        ddls = []
        for t in self.get_mview_list(schema):
            if drop_table:
                ddls.append('DROP TABLE {};'.format(t))
                if lower:
                    ddls.append('DROP TABLE {};'.format('"' + t.lower() + '"'))
            ddls.append(self.get_mview_ddl(t, schema))
            ddls.append('')
        return ddls

    def to_csv(self, table, fname, sep = 'chr(0)', schema='public'):
        cur = self._conn.cursor()

        qry = '''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{}'
                AND table_name   = '{}'
                order by ordinal_position
                '''.format(schema, table)

        cur.execute(qry)

        col_list = []
        for column_name, data_type in cur.fetchall():
            if data_type == 'timestamp with time zone':
                col_list.append("to_char({}, 'yyyy-mm-dd hh24:mi:ss.us') as {}". format(column_name, column_name))
            else:
                col_list.append('"{}"'.format(column_name))

        qry = "COPY (select {} from {}) TO STDOUT WITH DELIMITER {}".format(','.join(col_list).rstrip(','), table, sep)
        log.info('Unloading table {} to file {}'.format(table, fname))
        f = open(fname, 'w')

        cur.copy_expert(qry, f)
        cur.close()
        f.close()

    def mview_to_csv(self, table, fname, sep = r"E'\t'", schema='public'):
        cur = self._conn.cursor()

        qry = '''
                SELECT  a.attname as col,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) as typ
                    FROM pg_attribute a
                      JOIN pg_class t on a.attrelid = t.oid
                      JOIN pg_namespace s on t.relnamespace = s.oid
                    WHERE a.attnum > 0
                      AND NOT a.attisdropped
                      AND t.relname = '{}' --<< replace with the name of the MV
                      AND s.nspname = '{}' --<< change to the schema your MV is in
                    ORDER BY a.attnum
                '''.format( table, schema)

        cur.execute(qry)

        col_list = []
        for column_name, data_type in cur.fetchall():
            if data_type.lower().find('time zone') > 0:
                col_list.append("to_char({}, 'yyyy-mm-dd hh24:mi:ss.us') as {}". format(column_name, column_name))
            else:
                col_list.append('"{}"'.format(column_name))

        qry = "COPY (select {} from {}) TO STDOUT WITH DELIMITER {}".format(','.join(col_list).rstrip(','), table, sep)
        log.debug(qry)
        log.info('Unloading table {} to file {}'.format(table, fname))
        f = open(fname, 'w')

        cur.copy_expert(qry, f)
        cur.close()
        f.close()


    def db_to_csv(self, datadir, schema = 'public', sep = 'chr(0)'):
        for t in self.get_table_list(schema):
            fname = os.path.join(datadir,'{}.{}.csv'.format(schema, t))
            log.info('Saving table: {} to csv path : {}'.format(t, fname))
            self.to_csv(t, fname, sep)



if __name__ == '__main__':

    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    data_dir = os.path.join(root_dir, 'data')
    print(root_dir)

    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(logging.StreamHandler(sys.stdout))
    pdb = PostgreDB()
    #pdb.test_connection()

    conn = pdb.connect()

    #pdb.db_to_csv(data_dir)
    #pdb.to_csv('productionconsumption', r'D:\05.DEV_REPOS\pg_to_nz\data\public.productionconsumption.csv', sep=r"E'\t'" )
    pdb.mview_to_csv('dailycashflow', r'D:\05.DEV_REPOS\pg_to_nz\data\public.dailycashflow.csv', sep=r"'|'")

    conn.close()