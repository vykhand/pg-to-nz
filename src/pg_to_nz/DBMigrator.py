import pg_to_nz.PostgreDB as pgdb
import pg_to_nz.NZ as nzdb
import logging, sys, os
import yaml

log = logging.getLogger()
cfg_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'config.yml')
cfg = yaml.load(open(cfg_file,'r'))

class DBMigrator:
    def __init__(self):
        self._root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self._data_dir = os.path.join(self._root_dir, 'data')
        self._log_dir = os.path.join(self._root_dir, 'data', 'logs')

        self._sep = '|'
        self._null_val = '\N'
        self._boolstyle = 't_f'

        self._pg = pgdb.PostgreDB()
        self._pg_conn = self._pg.connect()
        self._nz = nzdb.NZ()
        self._nz.connect()


    def migrate_ddl(self, drop_table = False, raise_error = True, lower = False):
        pg =  self._pg
        nz = self._nz

        for s in pg.get_db_ddl(drop_table = drop_table, lower = lower):
            nz.run_ddl(s, raise_error)

    def migrate_table(self, table, trunc_tables = False, overwrite_files = False, schema = 'public'):
        if trunc_tables:
            self._nz.run_ddl('TRUNCATE TABLE ' + table, raise_error=False)
        fname = os.path.join(self._data_dir, '{}.{}.csv'.format(schema, table))
        if (not os.path.exists(fname)) or overwrite_files:
            self._pg.to_csv(table, fname, self._sep)

        self._nz.load_table(table, fname, logdir=self._log_dir, sep=self._sep,
                            null_val=self._null_val, boolstyle=self._boolstyle)

    def migrate_data(self, trunc_tables = False, overwrite_files = False, schema = 'public'):

        for t in self._pg.get_table_list(schema):
            self.migrate_table(t, trunc_tables, overwrite_files, schema)

    def rename_to_lower(self, schema = 'public'):
        '''this is a dirty hack because mondrian wants names in lower case'''
        for t in self._pg.get_table_list(schema):
            self._nz.run_ddl('ALTER TABLE {} RENAME TO "{}"'.format(t, t.lower()))



if __name__ == '__main__':
    print(os.path.dirname(os.path.dirname(__file__)))

    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(logging.StreamHandler(sys.stdout))
    mg = DBMigrator()

    mg.migrate_ddl(drop_table = True, raise_error=False)
    mg.migrate_data(trunc_tables=True, )

    #mg.migrate_table('fintransactions', trunc_tables=True,overwrite_files=True )
