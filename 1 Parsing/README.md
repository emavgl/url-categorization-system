DEPENDENCIES:
- boilerpipe
- aiohttp
- asyncio

How to use:
python3 parser.py dataset_1.txt

Output:
dataset_1.txt.log <- logfile
dataset_1.txt.db <- sqlite db


DATABASE STRUCTURE:

CREATE TABLE `urltable` (
	`lat`	REAL NOT NULL,
	`long`	REAL NOT NULL,
	`url`	TEXT NOT NULL,
	`boilerpipe`	TEXT,
	`keywords`	TEXT,
	PRIMARY KEY(`lat`,`long`,`url`))
