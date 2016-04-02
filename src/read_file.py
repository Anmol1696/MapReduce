"""
    In this module the file is read into chunks of lines at a time for each cluster
"""

from itertools import islice

def read_file(num_lines_per_node, num_nodes, rank, file_obj, num_last_line=0):
    """
        This is for a cluster. For each cluster it will read files as genrator

        cluster_rank -> Rank of the cluster(in this case number of PC's)
        num_nodes -> Number of nodes in one cluster
        num_lines_per_node -> Number of lines of the main file which will be read
            Not yet considering uneqaul size of the same file in that cluster.

        file_obj is the result of with open(..) as file_obj, this function should be called from a block wrapped by the above line
        num_last_line -> number of lines for the last node
    """
    start_line = rank*num_lines_per_node

    if not num_last_line and rank = num_nodes - 1:
        node_iterator = islice(file_obj, start_line, num_lines_per_node)
    else:
        node_iterator = islice(file_obj, start_line, num_last_line)

    #start_line += num_lines_per_node
    return node_iterator
