"""
    This module we mpi for running in different threads    
"""

from mpi4py         import MPI
from read_file      import read_file
from mapping_func   import mapping_func, reducing_func

import os
import numpy as np

def count_lines(file_name):
    """
        Finding the total number of lines in the given file
    """
    num_lines = sum(1 for line in file_name)

    return num_lines

def get_words():
    """
        Get the list of words for which the word count is used
        for testing just,
    """
    words = np.array(['this', 'a'])

    return words

def main_work(file_name, word):
    """
        This functions performs the main_func of dividing the work and all
    """

    #MPI.Init()
    comm = MPI.COMM_WORLD
    
    size = comm.Get_size()
    rank = comm.Get_rank()
    
    #with open(file_name) as file_obj:
    lines_data = []
    if rank == 0:
        num_lines       = count_lines(file_name)
        lines_per_node  = num_lines/size
        lines_last_node = num_lines - (lines_per_nodes*size)

        words = get_words()
        lines_data = np.array([num_lines, lines_per_node, lines_last_node])

        for i in range(1, num_nodes-1):
            comm.send(words, dest = i, tag=0)

    else:
        lines_data = np.array([0, 0, 0, 0])
        words = comm.recv(source=0, tag=0)

    comm.Bcast(lines_data, root=0)

    with open(file_name) as file_obj:
        node_iterator = read_file(lines_data[1], size, rank, file_obj, lines_data[2])
        mapped = mapping_func(node_iterator, words, case=False)

    if rank != 0:
        comm.send(mapped, dest=0, tag=1)
    else:
        mapped_list = np.array([comm.recv(source=i, tag=1) for i in range(1, size-1)])
        
        # Performing reducing function (for now only on the master node)
        mapped_list.append(mapped)
        final_dict = reducing_func(mapped_list)

