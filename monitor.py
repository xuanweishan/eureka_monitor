"""
Usage:
    monitor.py [arguments]

Arguments:
    -o [output_file],  --output [output_file]   Specify output file path and name.
    -n [node_names],   --nodes  [node_name]     Specify nodes you want to check, saperated by ",".
    
    -h                 Print this help message.

Return: The list of state on each, the including state data as below

node_name  user  job_name job_ID Time %CPU CPU_Mem T_CPU %GPU GPU_Mem T_GPU v_IB disk_usage
"""
import os
import subprocess
import argparse

from os.path import isfile


def arg_handler():
    """
    Handle the arguments of the script


    Return:

    args : class argparse.Namespace
    """

    Help_message = """
Return: The list of state on each, the including state data as below

node_name  user  job_name job_ID Time %CPU CPU_Mem T_CPU %GPU GPU_Mem T_GPU v_IB disk_usage
                   """
    
    parser = argparse.ArgumentParser( description = "Eureka node state monitor",
                                      formatter_class = argparse.RawTextHelpFormatter,
                                      epilog = Help_message,
                                    )
    parser.add_argument('-o', '--output',
                        help = "Specify output file.",
                        type = str,
                        default = 'std',
                       )

    parser.add_argument( '-n', '--nodes',
                         help = "Specify nodes you want to check, saperated by ','.",
                         type = str,
                         default = 'all'
                       )
    args, unknown = parser.parse_known_args()
    return args

def check_node_state():
    """
    Check alive nodes from pbs.

    Return:

    alive_nodes : list of alive nodes
    """

    #dead_nodes = subprocess()

if __name__ == "__main__":
    # 1. Handle arguments
    args = arg_handler()

    # 2. Get node state : alive_or_dead user job_name job_ID Time
    node_state = check_node_state()

    # 3. Get node datas
    # 3.2 CPU : Usage and Temp.
    # 3.3 Memory usage
    # 3.4 GPU : Usage Mem_Usage Temp.
    # 3.5 IB : Status Speed
    # 3.6 Disk : Usage

    # 4. Merge data as a table

    # 5. Output
