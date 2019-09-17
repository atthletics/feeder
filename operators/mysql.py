import os, yaml
import MySQLdb
import logging as log
log.basicConfig(format='MySQL | %(asctime)s | %(levelname)s | %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p',
                level=log.INFO)

class DictListToMySQL():
    def __init__(self, game_dicts, table):
        self.game_dicts = game_dicts
        self.table = table
        log.info('Loading data to MySQL table: ' + self.table)
        db_config_path = os.path.abspath(
            os.path.join(os.path.dirname( __file__ ), '..', 'db_config.yaml'))
        with open(db_config_path, 'r') as f:
            db_config = yaml.load(f)
        self.db = MySQLdb.connect(
            host = db_config['host'],
            user = db_config['user'],
            passwd = db_config['passwd'],
            db = db_config['db'])
        self.cursor = self.db.cursor()
        self.main()

    def generate_delete(self):
        scrape_ts = self.game_dicts[0]['scrape_ts'].strftime("%Y-%m-%d %H:%M:%S")
        log.info('Deleting Data For Scrape Timestamp: ' + scrape_ts)
        del_sql_tmpl = "DELETE FROM {table} WHERE scrape_ts = '{scrape_ts}';"
        del_params = {
            'table'     : self.table,
            'scrape_ts' : scrape_ts
        }
        self.del_sql = del_sql_tmpl.format(**del_params)
        return(self.del_sql)

    def generate_insert(self):
        self.columns = list(self.game_dicts[0].keys())
        log.info('Inserting columns')
        print(*self.columns, sep='\n')
        self.data = [tuple(game.values()) for game in self.game_dicts]
        log.info('Loading Data:') 
        print(*self.data, sep='\n')
        n_cols = len(self.columns)
        vals = ', '.join(['%s'] * n_cols)
        insert_params = {
            'table': self.table,
            'columns': ', '.join(self.columns),
            'vals': vals
        }
        insert_sql_tmpl = 'INSERT INTO {table} ({columns}) VALUES ({vals});'
        self.insert_sql = insert_sql_tmpl.format(**insert_params)
        return(self.insert_sql)

    def main(self):
        self.generate_delete()
        self.cursor.execute(self.del_sql)
        self.db.commit()

        self.generate_insert()
        self.cursor.executemany(self.insert_sql, self.data)
        self.db.commit()
        log.info('Done')
