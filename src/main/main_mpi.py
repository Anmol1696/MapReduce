"""
    This module we mpi for running in different threads    
"""

from src.mpi4py         import MPI
from src.read_file      import read_file
from src.mapping_func   import mapping_func, reducing_func
from src.helping_func   import clear_cache, get_approx_file_size

import os
import time
import numpy as np
"""
def count_lines(file_name):
    
        #Finding the total number of lines in the given file
    
    num_lines = sum(1 for line in open(file_name))

    return num_lines

def get_words():
    
    #    Get the list of words for which the word count is used
    #    for testing just,
    
    words = np.array(['the', 'a'])

    return words
"""
def main_work(file_name, cluster_size):
    """
        This functions performs the main_func of dividing the work and all
        cluster_rank -> rank of the cluster
    """

    comm = MPI.COMM_WORLD
    
    size = comm.Get_size()
    rank = comm.Get_rank()

    cluster_rank = 0
    
    lines_data = []
    if rank == 0:
        num_lines       = get_approx_file_size(file_name)
        lines_data      = np.array([num_lines, num_lines/cluster_size])
        #print 'Lines data -> ', lines_data
    else:
        lines_data = np.array([0, 0])
        words = comm.recv(source=0, tag=0)

    comm.Bcast(lines_data, root=0)

    with open(file_name) as file_obj:
        node_iterator = read_file(lines_data[1], size, cluster_rank, file_obj)
        mapped = mapping_func(node_iterator, words, case=False)
    
    if rank != 0:
        comm.send(mapped, dest=0, tag=1)
    else:
        mapped_list = [comm.recv(source=i, tag=1) for i in range(1, size)]
        
        # Performing reducing function (for now only on the master node)
        mapped_list.append(mapped)
        final_dict = reducing_func(mapped_list)

        #printing
        print 'Final_result -> \n', final_dict

    print 'Done with rank -> ', rank

if __name__ == '__main__':
    main_work('data/file.txt')
