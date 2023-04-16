from pyspark import SparkConf, SparkContext
from operator import add
import sys
import math

def print_sample(*args, **kwargs):
    print(50*"-")
    for arg in args:
        print(arg)
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print(50*"-")

def read_cmd_args():
    # Read command line arguments
    try:
        data_file = sys.argv[1]
        query_word = sys.argv[2]
        query_word = query_word.lower()
        k = int(sys.argv[3])
        stopword_file = sys.argv[4]
        return data_file, query_word, k, stopword_file
    except:
        raise Exception("Invalid command line arguments!")

def read_stopwords(stopword_file):
    with open(stopword_file, 'r') as f:
        stopwords = set([line.strip() for line in f if (line.strip() != "")])
    return stopwords

def preprocess_document(document, stopwords):
    # Tokenize the line, remove stopwords, and return a list of words
    # Convert all words to lowercase
    document = document.lower()
    document = "".join([ch for ch in document if (ch.isalpha() or ch.isspace())])
    words = [word for word in document.split() if word not in stopwords]
    return words


def co_occurence_mapper(document, query_word):
    is_query_word_present = (True if query_word in document else False)
    if not is_query_word_present:
        # return {
        #     word: 0
        #     for word in document if word != query_word
        # }
        return {}
    
    co_occurence_dict = {
        word: 1
        for word in document if word != query_word
    }
    return co_occurence_dict

def calculate_pmi_score(p_x_y, p_x, p_y, n):
    # Compute the PMI for each word, simplifying the formula, we get
    # pmi = (p(x,y)/N) / ((p(x)/N)*(p(y)/N)) = p(x,y) * N / (p(x)*p(y))
    if p_x_y == 0:
        return -math.inf
    
    value = (p_x_y * n) / (p_x * p_y)
    return math.log2(value)

def init_pyspark_application():
    # Initialize Spark
    conf = SparkConf()
    conf.setAppName('WordAssociationUsingSpark')
    # conf.setMaster("local")
    sc = SparkContext(conf=conf)
    # sc.setLogLevel("OFF")
    return sc

def finish_pyspark_application(sc):
    # Stop Spark
    sc.stop()

def main():
    sc = init_pyspark_application()
    data_file, query_word, k, stopword_file = read_cmd_args()

    # print_sample(
    #     data_file=data_file, 
    #     query_word=query_word, 
    #     k=k, 
    #     stopword_file=stopword_file
    # )

    stopwords = read_stopwords(stopword_file)
    # print_sample(stopwords=stopwords)

    documents = sc.textFile(data_file)
    documents_rdd = documents.map(lambda doc: preprocess_document(doc, stopwords)).cache()
    # print_sample(documents_rdd_first_5=documents_rdd.take(5))

    # Value of N
    num_documents = documents_rdd.count()
    # print_sample(num_documents=num_documents)

    documents_with_unique_words_list_rdd = documents_rdd.map(lambda doc_word_list: set(doc_word_list))
    # print_sample(documents_with_unique_words_list_rdd_first_5=documents_with_unique_words_list_rdd.take(5))
    
    word_present_in_documents_count = documents_with_unique_words_list_rdd.flatMap(lambda x:list(x)).countByValue()
    # print_sample(word_present_in_documents_count=word_present_in_documents_count)

    #  Calculate the count of the query word in this word_present_in_documents_rdd
    # if the word is not present at all, set the value to 0
    query_word_count = word_present_in_documents_count.get(query_word, 0)
    # print_sample(query_word_count=query_word_count)

    # Compute co-occurrence between query_word and all other words in the list
    co_occurence_dicts_rdd = documents_rdd.map(lambda doc: co_occurence_mapper(doc, query_word))
    # print_sample(co_occurence_dicts_rdd_first_5=co_occurence_dicts_rdd.take(5))

    # Sum up the co-occurrence counts for each word
    co_occurence_counts_rdd = co_occurence_dicts_rdd.flatMap(lambda x:x.items()).reduceByKey(add)
    # print_sample(co_occurence_counts_rdd_first_5=co_occurence_counts_rdd.take(5))

    pmi_rdd = co_occurence_counts_rdd.map(lambda x: (x[0], calculate_pmi_score(x[1], query_word_count, word_present_in_documents_count.get(x[0]), num_documents))) 
    # print_sample(pmi_rdd_first_5=pmi_rdd.take(5))

    # Sort the PMI values in descending order
    pmi_rdd_sorted = pmi_rdd.sortBy(lambda x: x[1], ascending=False)

    pmi_ordered_words = pmi_rdd_sorted.collect()

    print_sample()

    if query_word_count == 0:
        print(f"Query word '{query_word}' is not present in the corpus. Please try again with a different query word.")

    # Print the top k words with the highest PMI values
    print(f"Top {k} positively asssociated words with the query word '{query_word}' are:")
    for i, (word, pmi_value) in enumerate(pmi_ordered_words[:k], 1):
        print(f"{i}. Word: {word}, PMI Score: {pmi_value}")

    print("")

    # Print the bottom k words with the lowest PMI values
    print(f"Top {k} negatively associated words with the query word '{query_word}' are:")
    for i, (word, pmi_value) in enumerate(pmi_ordered_words[::-1][:k], 1):
        print(f"{i}. Word: {word}, PMI Score: {pmi_value}")
    
    print_sample()

    finish_pyspark_application(sc)

if __name__ == '__main__':
    main()