import logging
import copy
import gensim
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words

class LDAHelper:
    """
    A set of methods to perform LDA over a set of document using
    - baseline method
    - heuristic merging
    - lda.update method
    """
    def __init__(self, passes=10, ntopics=5, wordspertopic=5, disableLogs=False):
        self.passes = passes
        self.ntopics = ntopics
        self.wordspertopic = wordspertopic
        
        if not disableLogs:
            logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

        # init tokenizer
        self.tokenizer = RegexpTokenizer(r'\w+')

        # init_stop_words
        it_stop = get_stop_words('it')
        en_stop = get_stop_words('en')
        custom_stop_words = ['de']
        self.stop_words = it_stop + en_stop + custom_stop_words


    def extract_topic_words(self, tops):
        """
        Input: list of topics with associated coherence
        Output: list of topics (only words)
        """
        words_topics = []
        for topic_and_coherence in tops:
            topic, _ = topic_and_coherence
            words_topic = []
            for word_and_prob in topic:
                words_topic.append(word_and_prob[1])
            words_topics.append(words_topic)
        return words_topics


    def similarity(self, words_topic_1, words_topic_2):
        """
        Compute the similarity between two list of words
        """
        sim = 0
        for word in words_topic_1:
            if word in words_topic_2:
                sim += 1
        return sim

    def merge_topic_lists(self, wtopics1, wtopics2):
        """
        Merge two topic lists together.
        If it finds the same topic in both lists,
        it keeps only one of them with the sum of
        the weights.
        """
        new_topics = []
        for wtopic1 in wtopics1:
            topic1, weight_1 = wtopic1
            new_w = weight_1
            for wtopic2 in wtopics2:
                topic2, weight_2 = wtopic2
                if topic1 == topic2:
                    new_w += weight_2
                    break
            new_topics.append((topic1, new_w))
        return new_topics


    def calculate_topic_distributions(self, model, documents):
        """
        Input: model, documents (set of bow)
        Output: a list with (topic_index, sum_of_distributions)
        """
        top_topics = [0] * (self.ntopics)
        for document in documents:
            dist = model.get_document_topics(document, minimum_probability=0)
            for doc_id in range(self.ntopics):
                top_topics[doc_id] += dist[doc_id][1]

        to_return = []
        for i, top in enumerate(top_topics):
            to_return.append((model.show_topic(i, 5), top))

        return sorted(to_return, key=lambda x: x[1], reverse=True)

    def clean(self, doc):
        """
        Convert the document into bag of words
        Tokenization + Stop Words removal
        Stemmatization is not performed.
        """
        # clean and tokenize document string
        raw = doc.lower()
        tokens = self.tokenizer.tokenize(raw)

        # remove stop words and numbers from tokens
        stopped_tokens = [
            i for i in tokens if i not in self.stop_words and not i.isdigit()]

        # remove word with length < 3
        pruned = [i for i in stopped_tokens if len(i) > 3]

        return pruned

    def create_dictionary(self, corpus, dictionary_filters=False):
        """
        Create a gensim.corpora.Dictionary from a set of bow (corpus)
        """
        dic = gensim.corpora.Dictionary(corpus)
        if dictionary_filters:
            dic.filter_extremes(no_below=5, no_above=0.8)
        return dic

    def lda_topic(self, documents, dictionary=None, dictionary_filters=False):
        """
        Input: set of strings (documents)
        Output: dictionary with {dictionary, corpus, lda_model, topics}
        Use the baseline model.
        """
        dic = dictionary
        if dic is None:
            dic = gensim.corpora.Dictionary(documents)
        if dictionary_filters:
            dic.filter_extremes(no_below=5, no_above=0.8)
        corpus = [dic.doc2bow(text) for text in documents]
        lda_model = gensim.models.ldamulticore.LdaMulticore(corpus,
                                                            num_topics=self.ntopics,
                                                            id2word=dic, passes=self.passes)
        topics = self.calculate_topic_distributions(lda_model, corpus)
        return {'dictionary': dic, 'corpus': corpus, 'lda_model': lda_model, 'topics': topics}

    def lda_toptopic_merge(self, lda_model, corpus_list):
        """
        Input: lda_model, to merge corpuses (corpus1 and corpus2) and final_corpus to compute probs
        Output: topics with associated probability distributions
        """
        top_topics = []
        for corpus in corpus_list:
            top_topics_area = self.calculate_topic_distributions(lda_model, corpus)
            top_topics.append(top_topics_area)
        
        merged_top_topics = top_topics[0]
        for top in top_topics[1:]:
            merged_top_topics = self.merge_topic_lists(merged_top_topics, top)
        	
        merged_top_topics = sorted(merged_top_topics, key=lambda x: x[1], reverse=True)
        
        return  merged_top_topics[:self.ntopics]

    def lda_update_merge(self, lda_model1, new_corpus, total_corpus):
        """
        Perform merge using lda.update(new_corpus)
        Input: lda_model, new_corpus
        Output: topics with associated probability distributions
        """
        lda_model = copy.deepcopy(lda_model1)
        lda_model.update(new_corpus)
        top_topics_area_tot_update = self.calculate_topic_distributions(lda_model, total_corpus)
        return {'lda_model': lda_model, 'topics': top_topics_area_tot_update}
