"""
    This contains some important functions used throughout
"""

import os

def clear_cache():
    """
        Clears the page cache inorder to free some memory
    """
    try:
        os.system('sudo sh -c "sync; echo 1 > /proc/sys/vm/drop_caches"')
    except:
        print 'Error not able to clear the cache'

    return 0

def get_approx_file_size(file_name, n=10000, default_size=1046728946):
    """
        Get approximate number of lines for a file
        default_size is the size of a file in bits
    """
    file_size = os.path.getsize(file_name)

    if file_size > default_size:
        os.system('head -%i %s > temp_file_approx'% (n, file_name))
        temp_size = os.path.getsize('temp_file_approx')
        os.system('rm temp_file_approx')
        
        return ((file_size*n/temp_size)*n)
    else:
        num_lines = sum(1 for line in open(file_name))

        return num_lines


