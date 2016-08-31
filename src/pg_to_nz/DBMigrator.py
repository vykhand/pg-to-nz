import pg_to_nz.PostgreDB as pgdb
import pg_to_nz.NZ as nzdb
import logging, sys, os
import yaml

log = logging.getLogger()
cfg_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'config.yml')
cfg = yaml.load(open(cfg_file,'r'))

class DBMigrator:
    def __init__(self, pg_sep = r"E'\t'", nz_sep = r"\t"):
        self._root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self._data_dir = os.path.join(self._root_dir, 'data')
        self._log_dir = os.path.join(self._root_dir, 'data', 'logs')

        self._pg_sep = pg_sep
        self._nz_sep = nz_sep
        self._null_val = '\N'
        self._boolstyle = 't_f'

        self._pg = pgdb.PostgreDB()
        self._pg_conn = self._pg.connect()
        self._nz = nzdb.NZ()
        self._nz.connect()


    def migrate_ddl(self, drop_table = False, raise_error = True, lower = False, views = False):
        pg =  self._pg
        nz = self._nz

        for s in pg.get_db_ddl(drop_table = drop_table, lower = lower, views = views):
            nz.run_ddl(s, raise_error)

    def migrate_table(self, table, trunc_tables = False, overwrite_files = False, schema = 'public'):
        if trunc_tables:
            self._nz.run_ddl('TRUNCATE TABLE ' + table, raise_error=False)
        fname = os.path.join(self._data_dir, '{}.{}.csv'.format(schema, table))
        if (not os.path.exists(fname)) or overwrite_files:
            self._pg.to_csv(table, fname, self._pg_sep)

        self._nz.load_table(table, fname, logdir=self._log_dir, sep=self._nz_sep,
                            null_val=self._null_val, boolstyle=self._boolstyle)

    def migrate_mview(self, table, trunc_tables = False, overwrite_files = False, schema = 'public'):
        if trunc_tables:
            self._nz.run_ddl('TRUNCATE TABLE ' + table, raise_error=False)
        fname = os.path.join(self._data_dir, '{}.{}.csv'.format(schema, table))
        if (not os.path.exists(fname)) or overwrite_files:
            self._pg.mview_to_csv(table, fname, self._pg_sep)

        self._nz.load_table(table, fname, logdir=self._log_dir, sep=self._nz_sep,
                            null_val=self._null_val, boolstyle=self._boolstyle)

    def migrate_mviews_as_tables(self,  drop_table = True, lower = False, raise_error = False,
                                 trunc_tables = True, overwrite_files = False, schema = 'public' ):
        pg =  self._pg
        nz = self._nz

        for mv in pg.get_all_mview_ddl(drop_table = drop_table, lower = lower):
            nz.run_ddl(mv, raise_error)

        for mv in pg.get_mview_list(schema):
            self.migrate_mview(mv, trunc_tables=trunc_tables, overwrite_files=overwrite_files, schema=schema)

    def migrate_data(self, trunc_tables = False, overwrite_files = False, schema = 'public', views = False):

        for t in self._pg.get_table_list(schema, views):
            self.migrate_table(t, trunc_tables, overwrite_files, schema)

    def rename_to_lower(self, schema = 'public', views = False):
        '''this is a dirty hack because mondrian wants names in lower case'''
        for t in self._pg.get_table_list(schema, views):
            self._nz.run_ddl('ALTER TABLE {} RENAME TO "{}"'.format(t, t.lower()))


    def rename_mviews_to_lower(self, schema='public'):
        '''this is a dirty hack because mondrian wants names in lower case'''
        for t in self._pg.get_mview_list(schema):
            self._nz.run_ddl('ALTER TABLE {} RENAME TO "{}"'.format(t, t.lower()))

if __name__ == '__main__':
    print(os.path.dirname(os.path.dirname(__file__)))

    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(logging.StreamHandler(sys.stdout))
    mg = DBMigrator()

    #mg.migrate_ddl(drop_table = True, raise_error=False)
    #mg.migrate_data(trunc_tables=True, )

    #mg.migrate_mviews_as_tables(overwrite_files=True)
    mg.rename_mviews_to_lower()
