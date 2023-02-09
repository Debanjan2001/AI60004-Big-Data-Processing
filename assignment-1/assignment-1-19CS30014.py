import os
from threading import Thread
from nltk import word_tokenize, FreqDist
from nltk.util import ngrams
import sys
import chardet
import math

def process_text_from_binary(binary_content):
    encoding_info = chardet.detect(binary_content)
    # print(encoding_info)
    if encoding_info['encoding']:
        text = binary_content.decode(encoding_info['encoding'])
    else:
        text = binary_content.decode('ascii')

    text = text.lower()
    filtered_text = ""
    for c in text:
        if c.isalnum():
            filtered_text += c
        else:
            filtered_text += " "

    return filtered_text

def get_n_grams_from_single_file(file_path, n_value):
    with open(file_path, "rb") as f:
        file_content = f.read()
    
    processed_text = process_text_from_binary(file_content)
    # print(file_path, processed_text)
    
    words = word_tokenize(processed_text)
    n_grams_list = list(ngrams(words, n_value))
    return n_grams_list


def get_n_grams_with_score_from_single_class(class_path, n_value, k_value, tid):
    print(75*"-")
    print(f"Thread-{tid} started working on class: {class_path}...")
    print(75*"-"+"\n")
    documents = os.listdir(class_path)
    num_documents = len(documents)
    
    n_grams_list = []
    for filename in documents:
        file_path = os.path.join(class_path, filename)
        n_grams_list.extend(get_n_grams_from_single_file(file_path, n_value))


    n_grams_with_frequency = FreqDist(n_grams_list).most_common(k_value)
    # print(n_grams_with_frequency)
    n_grams_with_score = [
        [n_gram, freq/num_documents] 
        for (n_gram,freq) in n_grams_with_frequency
    ]

    print(75*"-")
    print(f"Thread-{tid} finished working on class: {class_path}")
    print(75*"-"+"\n")

    return n_grams_with_score


def get_n_grams_from_chunks_of_classes(collection_classes_paths, start, end, classwise_results, n_value, k_value, tid):
    for i in range(start, end):
        class_path = collection_classes_paths[i]
        classwise_results[i] = get_n_grams_with_score_from_single_class(class_path, n_value, k_value, tid)

def get_top_k_ngrams_for_each_class(collection_path, num_threads, n_value, k_value):
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

def get_top_k_ngrams_for_collection(classwise_top_k_ngrams):
    pass

def main():
    try:
        collection_path = sys.argv[1]
        num_threads = int(sys.argv[2])
        n_value = int(sys.argv[3])
        k_value = int(sys.argv[4])
    except:
        print("Error! Please Correct Command Line Arguments")
        exit()
        
    assert(num_threads > 0)
    assert(n_value > 0)
    assert(k_value > 0)

    classwise_top_k_ngrams = get_top_k_ngrams_for_each_class(collection_path, num_threads, n_value, k_value)
    
    for k,v in classwise_top_k_ngrams.items():
        print(f"{k} ==> {v[:5]}")


def test():
    temp = get_n_grams_with_score_from_single_class("20_newsgroups/comp.os.ms-windows.misc", 3, 3, "Random")
    print(temp)
    
if __name__ == "__main__":
    # main()
    test()