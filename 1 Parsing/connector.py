import sqlite3
import logging
from pathlib import Path

class DbConnector:
    def __init__(self, filename):
        self.db_path = filename + ".db"
        self.logger = logging.getLogger(__name__)
        self.logger.info('start db connection')
        my_file = Path(self.db_path)

        self.conn = sqlite3.connect(self.db_path)
        self.c = self.conn.cursor()
        self.__createDb()

    def add(self, args):
        self.logger.info('insert: {}'.format(args[2]))
        self.c.execute("insert or ignore into urltable values (?, ?, ?, ?, NULL)", args)

    def commit(self):
        self.logger.info('commit');
        self.conn.commit()
        
    def close(self):
        self.conn.close()

    def __createDb(self):
        create_statement = '''
        CREATE TABLE if not exists `urltable` (
	`lat`	REAL NOT NULL,
	`long`	REAL NOT NULL,
	`url`	TEXT NOT NULL,
	`boilerpipe`	TEXT,
	`keywords`	TEXT,
	PRIMARY KEY(`lat`,`long`,`url`))
        '''
        self.logger.info('create db')
        self.c.execute(create_statement);
        self.conn.commit()

