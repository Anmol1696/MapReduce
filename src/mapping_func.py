"""
    Module for performing the mapping and the reduced fucntion
"""

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

def mapping_func(node_genrator, words, case):
    """
        This will be performing the mapping_function
        node_genrator is the genrator consisting of the lines from a file
        case is bool which will consider lower case or not
            -> True (will consider different case
            -> False (will perform the lower function on the line)
    """
    mapped = dict((word, 0) for word in words)

    for line in node_genrator:
        word_count(line, mapped, case)
    
    return mapped

def reducing_func(mapped_list)
    """
        Given a list of dict mapped dict
    """

    final_dict = reduce(lambda x, y: dict((k, v + y[k]) for k, v in x.iteritems()), mapped_list)

    return final_dict
