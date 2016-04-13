"""
    Module for performing the mapping and the reduced fucntion
"""

from collections import Counter
import numpy as np

def word_count(line, mapped, case):
    """
        line is a single line from a file
        words is a list of words for which we want to perform the word_count
        mapped is a dict of the form {word:count}, this has elements that are already mapped
        case is bool for considering upper case and lower case or not
    """
    if not case:
        line = line.lower()
    
    line = line.split()

    for word in mapped:
        mapped[word] += line.count(word)

def mapping_func(file_data):
    """
        file_data is the scattered file split with '\n' and is a numpy array
        mapped_data is a numpy array of the the form (word, 1)
    """
    print 'Here ->', len(file_data)
    
    final_mapped = np.array([word for line in file_data for word in line.split(' ')])

    return final_mapped

def reducing_func(mapped_data):
    """
        Given a np array of the mapped values
    """

    reduce_counter = Counter(mapped_data)
    final_reduce   = np.array([(word,val) for word, val in reduce_counter.iteritems()], dtype='|S15, i8') 
    
    return final_reduce
"""
def final_reduce_1(list_reduced_data):
    
    #    given a list of the reduced data from each node this will reduce
    
    reduced_final = []
    unique_words_index = []

    list_reduced = list_reduced_data[0]

    for reduced in list_reduced_data[1:]:
        list_reduced = np.concatenate((list_reduced, reduced))
    print 'Converted to numpy...'

    for word, val in list_reduced:
        index_array = np.where(list_reduced==word)[0]
        
        if str(index_array) not in unique_words_index:
            unique_words_index.append(str(index_array))
            val_sum = sum([int(list_reduced[i][1]) for i in index_array])
            reduced_final.append((word, val_sum))

    return np.array(reduced_final, dtype='|S15, i8')
"""
def final_reduce(list_reduced_data):
    """
        reduce the final by using dicts
    """
    final_dict = [Counter(dict(data)) for data in list_reduced_data]

    print 'Done with the dict'
    
    final_dict_data = Counter(dict())

    for data in final_dict:
        final_dict_data += data

    return np.array(zip(final_dict_data.keys(), final_dict_data.values()))
