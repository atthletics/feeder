import os, yaml
import MySQLdb

class DictListToMySQL():
    def __init__(self, game_dicts, table):
        self.game_dicts = game_dicts
        self.table = table
        with open('db_config.yaml', 'r') as f:
            db_config = yaml.load(f)
        self.db = MySQLdb.connect(
            host = db_config['host'],
            user = db_config['user'],
            passwd = db_config['passwd'],
            db = db_config['db'])
        self.cursor = self.db.cursor()
        #self.main()

    def generate_delete(self):
        scrape_ts = self.game_dicts[0]['scrape_ts'].strftime("%Y-%m-%d %H:%M:%S")
        del_sql_tmpl = "DELETE FROM {table} WHERE scrape_ts = '{scrape_ts}';"
        del_params = {
            'table'     : self.table,
            'scrape_ts' : scrape_ts
        }
        self.del_sql = del_sql_tmpl.format(**del_params)
        return(self.del_sql)

    def generate_insert(self):
        self.columns = list(self.game_dicts[0].keys())
        self.data = [tuple(game.values()) for game in self.game_dicts]
        n_cols = len(self.columns)
        vals = ', '.join(['%s'] * n_cols)
        insert_params = {
            'table': self.table,
            'columns': ', '.join(self.columns),
            'vals': vals
        }
        insert_sql_tmpl = 'INSERT INTO {table} ({columns}) VALUES ({vals});'
        self.insert_sql = sql_tmpl.format(**insert_params)
        return(self.insert_sql)

    def main(self):
        self.generate_delete()
        self.cursor.execute(self.del_sql)
        self.db.commit()

        self.generate_insert()
        self.cursor.executemany(self.insert_sql, self.data)
        self.db.commit()
