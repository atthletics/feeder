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
        self.main()

    def generate_insert(self):
        self.columns = list(self.game_dicts[0].keys())
        self.data = [tuple(game.values()) for game in self.game_dicts]
        n_cols = len(self.columns)
        vals = ', '.join(['%s'] * n_cols)
        sql_params = {
            'table': self.table,
            'columns': ', '.join(self.columns),
            'vals': vals
        }
        sql_tmplate = 'INSERT INTO {table} ({columns}) VALUES ({vals});'
        self.sql = sql_tmplate.format(**sql_params)
        print(self.sql)

    def main(self):
        self.generate_insert()
        self.cursor.executemany(self.sql, self.data)
        self.db.commit()
