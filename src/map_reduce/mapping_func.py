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
    final_reduce   = np.array([(word,val) for word, val in reduce_counter.iteritems()]) 
    
    return final_reduce
