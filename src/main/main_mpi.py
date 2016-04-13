"""
    This module we mpi for running in different threads    
"""

from mpi4py import MPI
import os
import time
import numpy as np


from src.file_manage.read_file              import read_file
from src.map_reduce.mapping_func            import mapping_func, reducing_func, final_reduce
from src.helping_functions.helping_func     import clear_cache, get_approx_file_size
from src.helping_functions.download_data    import call_get_data

def main_work(download=False):
    """
        This functions performs the main_func of dividing the work and all
        cluster_rank -> rank of the cluster
    """

    comm = MPI.COMM_WORLD
    
    size = comm.Get_size()
    rank = comm.Get_rank()

    cluster_rank = 0
    
    if rank == 0:
        if download:
            print 'Downloading data....'
            num_file = call_get_data()
            clear_cache()
            print 'Done downloading data'
        else:
            file_num = [2]
            file_name = 'file_'
            num_file = (file_num, file_name)

        for i in range(1,size):
            comm.send(num_file, dest=i, tag=9)

    else:
        num_file = comm.recv(source=0, tag=9)

    file_name = num_file[1]

    for file_num in num_file[0]:
        if rank == 0:
            print 'Reading file to ram'
            read_file = open('data/' + file_name + str(file_num) + '.txt').read()
            read_file = np.array(read_file.split('\n'))
            clear_cache()
            print 'Done'

            #Now will be scatterting this data into the othermpi's
            chunk = len(read_file)/size
            for i in range(1, size):
                if i != size - 1:
                    comm.send(read_file[chunk*i : chunk*(i+1)], dest=i, tag=11)
                else:
                    comm.send(read_file[chunk*i:], dest=i, tag=11)

            read_file = read_file[:chunk]

        else:
            read_file = comm.recv(source=0, tag=11)

        mapped_data = mapping_func(read_file)
        clear_cache()
     
        print 'mapped_data for the rank->', rank, 'Mapped len is ', len(mapped_data) 
    
        reduced_data = reducing_func(mapped_data)
        print reduced_data
        clear_cache()

        print 'reduced data from rank ', rank, 'reduce len is', len(reduced_data)

        if rank != 0:
            comm.send(reduced_data, dest=0, tag=15)
        else:
            list_reduced_data = [comm.recv(source=i, tag=15) for i in range(1, size)]
            list_reduced_data = np.array([reduced_data] + list_reduced_data)

            list_reduced_data = final_reduce(list_reduced_data)

            print 'Done with final reduce\nThe result is -> \n', list_reduced_data

    
if __name__ == '__main__':
    main_work()
