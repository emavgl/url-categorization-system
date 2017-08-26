import sys, logging
import sqlite3
import pymongo

def createDic(doc):
	"""
	Create the dictionary to be inserted in the database
	"""
	lat = doc[0]
	lon = doc[1]
	url = doc[2]
	boilerpipe = doc[3]
	loc = {"type": "Point", "coordinates": [lon, lat]}
	return {'url': url, 'boilerpipe': boilerpipe, 'loc': loc}

def getDocsFromDB(dbname):
	"""
	Get docs from the SQLite database
	"""
	conn = sqlite3.connect(dbname)
	c = conn.cursor()
	retrieve_query = "SELECT * FROM urltable"
	c.execute(retrieve_query)
	docs = c.fetchall()
	return docs
	

if len(sys.argv) != 2:
	print("Wrong syntax")
	print("python3 sqlite2mongo.py database.sql")
	sys.exit(-1)
	

docs = getDocsFromDB(sys.argv[1])
print("Loaded " + str(len(docs)) + " documents from db")

# connect db
client = pymongo.MongoClient('localhost', 27017)

# get db
db = client['url-project']

# open collection
documents = db['documents']

counter = 0
for doc in docs:
	try:
		dic = createDic(doc)
		post_id = documents.insert_one(dic).inserted_id
		counter = counter + 1
	except IndexError as e:
		print(e, "skip")
		continue

	if counter % 100 == 0:
		print("Computed " + str(counter) + "/" + str(len(docs)))
