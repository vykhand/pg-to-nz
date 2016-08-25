import logging, sys, os
import yaml
import subprocess
import pyodbc

log = logging.getLogger()
cfg_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'config.yml')
cfg = yaml.load(open(cfg_file,'r'))

class NZ:
    def __init__(self):
        self._nz_dir = cfg['nz_client_dir']
        self._nzload  = os.path.join(self._nz_dir, 'nzload.exe')
        self._nz_db = cfg['nz_db']
        self._nz_user = cfg['nz_user']
        self._nz_pwd = cfg['nz_pwd']
        self._nz_db = cfg['nz_db']
        self._nz_server = cfg['nz_server']


        log.debug('nzload location : ' + self._nzload)
    def connect(self):
        self._conn = pyodbc.connect('DRIVER={};SERVER={};DATABASE={};UID={};PWD={}'.format(cfg['nz_driver'],
                                                                                               cfg['nz_server'],
                                                                                               cfg['nz_db'],
                                                                                               cfg['nz_user'],
                                                                                               cfg['nz_pwd']))
        return self._conn
    def disconnect(self):
        self._conn.close()

    def load_table(self, table, fname, logdir = None,  sep = '|', null_val = '\N', boolstyle = 't_f', escapechar = '\\'):

        ldir = logdir or os.path.dirname(fname)

        logfile = os.path.join(ldir, os.path.basename(fname) + '.nzlog')
        badfile = os.path.join(ldir, os.path.basename(fname) + '.nzbad')

        cmd = [self._nzload,
               '-host ' + self._nz_server,
               '-db ' + self._nz_db,
               '-u ' + self._nz_user,
               '-pw ' + self._nz_pwd,
               '-delim ' + sep,
               '-nullValue ' + null_val,
               '-boolStyle ' + boolstyle,
               '-escapeChar ' + escapechar,
               '-t ' + table,
               '-lf ' + logfile,
               '-bf ' + badfile,
               '-df ' + fname,

               ]
        log.info('Running command : {} '.format( ' '.join(cmd)))
        subprocess.call(' '.join(cmd))

    def run_ddl(self, sql, raise_error = True):
        cur = self._conn.cursor()
        try:
            log.info('running statement: ')
            log.info(sql)

            cur.execute(sql)
            cur.commit()

            log.info('Success!')
        except pyodbc.ProgrammingError, e:
            log.warn('DDL failed: ' + str(e))
            if raise_error:
                raise e
        finally:
            cur.close()




if __name__ == '__main__':

    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(logging.StreamHandler(sys.stdout))


    nz = NZ()

    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    log_dir = os.path.join(root_dir, 'data', 'logs')
    nz.load_table('fintransactions', r'D:\05.DEV_REPOS\pg_to_nz\data\public.fintransactions.csv', logdir =  log_dir )

    # '-host "10.91.27.99"', '-db DB_RESEVO', '-u USR_RESEVO', '-pw passw0rd', '-t statement', '-df "D:\\05.DEV_REPOS\\pg_to_nz\\data\\public.statement.csv"',
    #subprocess.call(['C:/Program Files (x86)/IBM Netezza Tools/Bin/nzload.exe', '--host blala'])
