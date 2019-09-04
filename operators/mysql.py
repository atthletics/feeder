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
        self.cursor = db.cursor()

    def generate_insert(self):
        self.columns = list(self.game_dicts[0].keys())
        self.data = [tuple(game.values()) for game in self.game_dicts]
        n_cols = len(self.columns)
        vals = ', '.join(['%s'] * n_cols)
        sql_params = {
            'table': table,
            'columns': self.columns,
            'vals': vals
        }
        self.sql = 'INSERT INTO {table} ({columns}) VALUES {vals};'
        print(self.sql)

    def run(self):
        self.cursor.executemany(self.sql, self.data)
        db.commit()

    def main(self):
        sel.generate_insert()
        self.run()