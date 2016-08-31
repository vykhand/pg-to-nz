import pg_to_nz.PostgreDB as pgdb
import pg_to_nz.NZ as nzdb
import pg_to_nz.DBMigrator as dbm
import logging, sys, os
import yaml

log = logging.getLogger()
#cfg_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'config.yml')
#cfg = yaml.load(open(cfg_file,'r'))

if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(logging.StreamHandler(sys.stdout))

    mg = dbm.DBMigrator()

    mg.migrate_ddl(drop_table = True, raise_error=False, lower = True, views = True)
    mg.migrate_data(trunc_tables=True, overwrite_files=False, views=True)
    mg.rename_to_lower(views = True)