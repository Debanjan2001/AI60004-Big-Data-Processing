import os
from threading import Thread
from nltk.util import ngrams
from collections import Counter
import sys
import chardet
import math

def process_text_from_binary(binary_content):
    """
    Params:
    --------------
        binary_content: A binary file containing data
    
    Output:
    --------------
        Returns a processed text file in the format of a string

    Description:
    --------------
        This function:
        - Decodes the binary file into string using its appropritate encoding method
        - lowercases all the characters 
        - replaces the non-alphanumeric characters with blank spaces
        - returns the final string
    """

    # While testing, I found some files to have different encodings(other than ascii/utf-8)
    # Hence, I have used chardet library to determine the encoding of the file and then decode it using that encoding format
    try:
        text = binary_content.decode()
    except:
        encoding_info = chardet.detect(binary_content)
        # print(encoding_info)
        text = binary_content.decode(encoding_info['encoding'])

    text = text.lower()
    filtered_text = "".join([
        c if c.isalnum() else " "
        for c in text 
    ])

    return filtered_text

def get_n_grams_from_single_file(file_path, n_value):
    """
    Params:
    --------------
        file_path:  path to a file/document inside a class
        n_value:    value of n for generating n-grams 
    
    Output:
    --------------
        Returns n-grams from a single document

    Description:
    --------------
        This function:
        - reads the file from file_path
        - processes the text using `process_text_from_binary(...)` method
        - splits the string into words
        - generates and returns the list of n grams using nltk library
    """
    with open(file_path, "rb") as f:
        file_content = f.read()
    
    processed_text = process_text_from_binary(file_content)
    # print(file_path, processed_text)
    
    words = [word for word in processed_text.split(" ") if word!=""]
    n_grams_list = list(ngrams(words, n_value))
    return n_grams_list

def get_n_grams_with_score_from_single_class(class_path, n_value, k_value, tid):
    """
    Params:
    --------------
        class_path:  path to a folder which is a class of documents
        n_value:     value of n for generating n-grams
        k_value:     value of k for getting top_k n-grams
        tid:         an integer thread-id to know which thread is working on it
    
    Output:
    --------------
        Returns top-k n-grams from a single class based on their class-salience scores

    Description:
    --------------
        This function:
        - iterates through files present in a class from class_path
        - processes the file using `get_n_grams_from_single_file(...)` method
        - combines all the n_grams from each file into a list and gets the top-k n-grams from their frequency
        - Finally calculates their class-salience scores and returns it as a list of lists in the format: [n_gram, salience-score] 
        - generates and returns the list of n grams using nltk library
    
    Reasoning:
    --------------
        - To calculate the overall top-k n-grams from all classes, we do not need all the n-grams from each class
        - we can calculate top-k n-grams from each class and then compare among them to get the overall top-k n-grams
        - Furthermore, for one class, we can calculate the top-k n-grams by simply sorting them based on their counts 
    """
    class_name = str(class_path).split('/')[1]
    print(f"LOG:: Thread-{tid} started working on class: {class_name} ...")
    documents = os.listdir(class_path)
    num_documents = len(documents)
    
    n_grams_list = []
    for filename in documents:
        file_path = os.path.join(class_path, filename)
        n_grams_list.extend(get_n_grams_from_single_file(file_path, n_value))


    n_grams_with_frequency = Counter(n_grams_list).most_common(k_value)
    # print(n_grams_with_frequency)
    n_grams_with_score = [
        [n_gram, freq/num_documents] 
        for (n_gram,freq) in n_grams_with_frequency
    ]

    print(f"LOG:: Thread-{tid} finished working on class: {class_name}")

    return n_grams_with_score


def get_n_grams_from_chunks_of_classes(collection_classes_paths, start, end, classwise_results, n_value, k_value, tid):
    """
    Params:
    --------------
        collection_classes_paths:  path to a list of paths which is a collection of classes
        start:                     start index of paths from which this thread-`tid` will handle the classes
        end:                       end index of paths from which this thread-`tid` will handle the classes
        classwise_results:         A dictionary to store the classwise results in a multithreaded fashion
        n_value:                   value of n for generating n-grams
        k_value:                   value of k for getting top_k n-grams
        tid:                       an integer thread-id to know which thread is working on it

    Output:
    --------------
        Stores and returns top-k n-grams for a chunk of classes based on their class-salience scores

    Description:
    --------------
        This function:
        - iterates through the chunk of classes defined in the range [start, end) 
        - processes the class using `get_n_grams_with_score_from_single_class(...)` method
        - Finally Stores and returns top-k n-grams for a chunk of classes based on their class-salience scores
    """    
    for i in range(start, end):
        class_path = collection_classes_paths[i]
        classwise_results[i] = get_n_grams_with_score_from_single_class(class_path, n_value, k_value, tid)

def get_top_k_ngrams_for_all_classes_multithreaded(collection_path, num_threads, n_value, k_value):
    """
    Params:
    --------------
        collection_path: path to a folder containing the collection of classes
        num_threads:     user defined number of threads to spawn
        n_value:         value of n for generating n-grams
        k_value:         value of k for getting top_k n-grams

    Output:
    --------------
        Returns a dictionary top-k n-grams for each classes in the format : {class_name: list([n_gram, class_salience_score])}

    Description:
    --------------
        This function:
        - spawns `num_threads` number of threads and allots a chunk of classes to each thread to process n-grams
        - waits for all the threads to finish their tasks
        - Finally returns a dictionary top-k n-grams for each classes in the format : {class_name: list([n_gram, class_salience_score])}
    """
    threads = [None] * num_threads

    classwise_results = {}
    collection_classes = os.listdir(collection_path)
    num_collection_classes = len(collection_classes)
    
    for i in range(num_collection_classes):
        classwise_results[i] = []

    collection_classes_paths = [
        f"{collection_path}/{collection_classes[i]}" 
        for i in range(num_collection_classes)
    ]

    total_tasks_left = num_collection_classes
    tasks_distributed = 0
    for i in range(num_threads):
        chunk_size = int(math.ceil(total_tasks_left/(num_threads-i)))
        start = tasks_distributed
        end = start+chunk_size
        tid = i+1

        # print(i, chunk_size)
        tasks_distributed += chunk_size
        total_tasks_left -= chunk_size
        
        threads[i] = Thread(
            target=get_n_grams_from_chunks_of_classes, 
            args=(collection_classes_paths, start, end, classwise_results, n_value, k_value, tid)
        )
        threads[i].start()

    for i in range(num_threads):
        threads[i].join()
        # print(f"thread {i+1} joined")

    prettified_classwise_results = {}
    for id,n_grams in classwise_results.items():
        prettified_classwise_results[collection_classes[id]] = n_grams
    
    return prettified_classwise_results

def get_top_k_ngrams_for_collection(classwise_top_k_ngrams, k_value):
    """
    Params:
    --------------
        classwise_top_k_ngrams: a list of top-k n-grams for each class stored in a `dict`
        k_value:                value of k for getting top_k n-grams

    Output:
    --------------
        Returns the overall top-k n-grams for the whole collection

    Description:
    --------------
        This function:
        - simply does some post-processing and calculates k-top n-grams from the list of k-top n-grams of each class
        - If an n-gram appears in two different documents, we simply consider the n_gram having higher class_salience_score 
        - Sorts a list of [n_gram, class_salience_score, class_name] based on its score value in descending order
        - Finally returns the top-k items from this list as top-k n-grams for the overall collection.
    """
    n_grams_dict = {}
    for class_name, n_gram_with_score_list in classwise_top_k_ngrams.items():
        for n_gram, score in n_gram_with_score_list:
            if n_gram in n_grams_dict:
                prev_score = n_grams_dict[n_gram][0]
                if score > prev_score:
                    n_grams_dict[n_gram] = [score, class_name]
            else:
                n_grams_dict[n_gram] = [score, class_name]
    
    overall_ngrams = [
        [n_gram, score, class_name]
        for n_gram, [score, class_name] in n_grams_dict.items()
    ]

    overall_ngrams.sort(key=lambda x:x[1], reverse=True)
    return overall_ngrams[:k_value]

def main():
    """
    Main function for taking inputs and showing the outputs
    """
    try:
        collection_path = sys.argv[1]
        num_threads = int(sys.argv[2])
        n_value = int(sys.argv[3])
        k_value = int(sys.argv[4])
    except:
        print("Error! Please Enter Correct Command Line Arguments")
        exit()
        
    assert(num_threads > 0)
    assert(n_value > 0)
    assert(k_value > 0)

    classwise_top_k_ngrams = get_top_k_ngrams_for_all_classes_multithreaded(collection_path, num_threads, n_value, k_value)
    overall_top_k_ngrams = get_top_k_ngrams_for_collection(classwise_top_k_ngrams, k_value)

    # for k,v in classwise_top_k_ngrams.items():
    #     print(f"{k} ==> {v[:5]}")
    # print(overall_top_k_ngrams)

    print(100*"-")
    print("{:5s} {:40s} {:25s} {:20s}".format("Rank", "N_Gram", "Origin-ClassName", "Score",))
    print(100*"-")
    for i, [n_gram, score, class_name] in enumerate(overall_top_k_ngrams, 1):
        print("{:5s} {:40s} {:25s} {}".format(str(i), str(n_gram), class_name, score))

if __name__ == "__main__":
    main()
    # test()

# ------------------  Testing Code ----------------------------------------------
# def test():
#     """
#     A sample test function to test some functions
#     """
#     temp = get_n_grams_with_score_from_single_class("20_newsgroups/comp.os.ms-windows.misc", 3, 3, "Random")
#     print(temp)
    