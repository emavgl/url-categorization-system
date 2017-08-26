import sys
import logging, gensim, bz2
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
import numpy as np
import sqlite3
from itertools import islice
import pymongo

def createDic(doc, lda_topics, tfidf_topics, mostfrequent_topics):
    lat = doc[0]
    lon = doc[1]
    url = doc[2]
    boilerpipe = doc[3]
    loc = {"type": "Point", "coordinates": [lon, lat]}
    return {'url': url, 'boilerpipe': boilerpipe, 'loc': loc, "lda": lda_topics, "tfidf": tfidf_topics, "mfw": mostfrequent_topics}

def getDocsFromDB(dbname):
    conn = sqlite3.connect('train_dataset.db')
    c = conn.cursor()
    retrieve_query = "SELECT * FROM urltable"
    c.execute(retrieve_query)
    docs = c.fetchall()
    return docs

def clean(doc):
    # clean and tokenize document string
    raw = doc.lower()
    tokens = tokenizer.tokenize(raw)

    # remove stop words from tokens
    stopped_tokens = [i for i in tokens if not i in stop_words]

    # stem tokens
    stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]

    # return tokens
    return stemmed_tokens

def evaluateLda(doc_bow, lda):
    result = lda[doc_bow]
    topic_list = list(sorted(result, key=lambda x: x[1], reverse=True))
    return lda.show_topic(topic_list[0][0], 5)

def evaluateTfIdf(doc_bow, tfidf):
    result = tfidf[doc_bow]
    result = sorted(result, key=lambda ele: ele[1], reverse=True)
    topics = []
    for word in islice(result, 5):
        topics.append((dictionary.get(word[0]), word[1]))
    return topics

def evaluateMostFrequentWords(doc_bow):
    result = sorted(doc_bow, key=lambda ele: ele[1], reverse=True)
    topics = []
    for word in islice(result, 5):
        topics.append((dictionary.get(word[0]), word[1]))
    return topics

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# init tokenizer
tokenizer = RegexpTokenizer(r'\w+')

# init_stop_words
it_stop = get_stop_words('it')
en_stop = get_stop_words('en')
stop_words = it_stop + en_stop

# Create p_stemmer of class PorterStemmer
p_stemmer = PorterStemmer()

# load dictionary from wiki_it_wordids
path_dictionary = "./gensim/results/wiki_it_wordids.txt"
dictionary = gensim.corpora.Dictionary.load_from_text(path_dictionary)

lda = gensim.models.ldamodel.LdaModel.load("checkpoint.lda.saved", mmap='r')
tfidf = gensim.models.tfidfmodel.TfidfModel.load('wikipedia_it.tfidf_model')

# set a random seed for np
np.random.seed(0)

docs = getDocsFromDB('train_dataset.db')
logging.info("Loaded " + str(len(docs)) + " documents from db")

# connect db
client = pymongo.MongoClient('localhost', 27017)

# get db
db = client['url-project']

# open collection
documents = db['documents']

counter = 0
for doc in docs:
    text = doc[3] # get boilerpipe
    try:
        cleaned_text = clean(text)
        doc_bow = dictionary.doc2bow(cleaned_text)
        lda_topics = evaluateLda(doc_bow, lda)
        tfidf_topics = evaluateTfIdf(doc_bow, tfidf)
        mostfrequent_topics = evaluateMostFrequentWords(doc_bow)
        dic = createDic(doc, lda_topics, tfidf_topics, mostfrequent_topics)
        post_id = documents.insert_one(dic).inserted_id
        counter = counter + 1
    except IndexError:
        continue

    if counter % 100 == 0:
        logging.info("Computed " + str(counter) + "/" + str(len(docs)))
