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


def validate_nodes(nodes, parser):
    """
    Verify node names are legal.
    Modify the digits to full node names

    Return:
    
    None
    """
    vali_nodes = []

    for node in nodes:
        if node.startswith('eureka') and int(node[6:]) < 34:
            vali_nodes.append(node)
            
        elif node.isdigit() and int(node) < 34:
            vali_nodes.append('eureka%02i' %int(node))

        else:
            print("Warning: Invalid node names\n")
            parser.print_help()
            exit(1)

    return vali_nodes

def arg_handler():
    """
    Handle the arguments of the script


    Return:

    args: class argparse.Namespace
    """
    
    # 1. Make help message
    Help_message = """
Return: The table of state on each nodes, the including state data as below:

node_name  user  job_name job_ID Time %CPU CPU_Mem T_CPU %GPU GPU_Mem T_GPU v_IB disk_usage
                   """
    # 2. Setup argparser

    # 2.1 Create an argpaser as parser 
    parser = argparse.ArgumentParser( description = "Eureka node state monitor",
                                      formatter_class = argparse.RawTextHelpFormatter,
                                      epilog = Help_message,
                                    )

    # 2.2 Add arguments into parser

    parser.add_argument('-o', '--output',
                        help = "Specify output file.",
                        type = str,
                        default = None,
                        required = False,
                       )

    parser.add_argument( '-n', '--nodes',
                         help = """
Specify nodes you want to check, saperated by ' '.
Can use only digit and full name.""",
                         type = str,
                         nargs = '+',
                         required = False,
                         default = None,
                       )

    # 3. Handle the input arguments

    args, unknown = parser.parse_known_args()

    # 4. Deal with argument results

    # 4.1 Stop process if unknown argument occurred
    if unknown:
        print("Warning: unexpected argument occurred.\n")
        parser.print_help()
        exit(1)
    
    # 4.2 If no specified nodes, then do it on all nodes.
    if args.nodes is None:
        args.nodes = ['eureka%02i' %(n) for n in range(0,34)]
    else:
        args.nodes = validate_nodes(args.nodes, parser)

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
    print(args)

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
