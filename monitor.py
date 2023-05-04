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
import io
import re
import subprocess
import argparse
import threading

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

def run_cli(cmd, results):
    try:
        result = subprocess.check_output(cmd)
        results[cmd[0]] = result.decode('utf-8').strip()
    except subprocess.CalledProcessError as err:
        print("Error: error occurred while running %s." %(cmd))
        print("    Error code: ", err.returncode)
        print("    Fail message:")
        print(err.output.decode('utf-8'))
        exit(1)

def get_node_state():
    """
    Check alive nodes from pbs.

    Return:

    alive_nodes : list of alive nodes
    """
    # 1. Command lines to get the node stat data
    qstat = ['qstat', '-n']
    pbsnodes = ['pbsnodes', '-a']
    cmds = [qstat, pbsnodes]
    
    # 2. Run cmds in multiple threads
    threads = []
    results = {}
    for cmd in cmds:
        thread = threading.Thread(target=run_cli, args=(cmd, results))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    
    # 3. Deal with command results
    # 3.1 qstat
    job_stats = qstat_data_handler(results['qstat'])
    print(job_stats)

    # 3.2 pbsnodes
    nodes_state = pbsnodes_data_handler(results['pbsnodes'])
    print(nodes_state)

    
def qstat_data_handler(qstat_msg):
    """
    Deal with message from command 'qstat -n'
    
    input:
    qstat_msg : result string from the command

    output:
    job_state : A list of directiionaries to record job stat 
    [{'Job_ID': ***, 'Name': ***, 'User', 'Time': **:**:**, 'Nodes': []}, ]
    """
    # 1. Split total message into lines
    lines = qstat_msg.splitlines()[4:]
    
    # 2. Deal with message per line
    # 2.1 Initialization of variables required
    nodes = []
    jobs_state = []

    # 2.2 Analyze message
    for line in lines:
        parts = line.split()
        # 2.2.1 Create new job info mation as read a new job.
        
        if len(parts) == 11:
            # 2.2.1.1 Add using nodes to previous job state
            if len(jobs_state) > 0:
                jobs_state[-1]['nodes'] = nodes

            # 2.2.1.2 Create a new job state
            job_ID = parts[0].split('.')[0]
            user = parts[1]
            name = parts[3]
            stat = parts[9]
            time = parts[10]
            nodes = []
            jobs_state.append({
                               'ID': job_ID,
                               'User': user,
                               'Name': name,
                               'Time': time,
                               'Stat': stat,
                             })
        
        # 2.2.2 Grep Using nodes in the job
        elif len(parts) == 1:
            threads = line.split('+')
            for thread in threads:
                if thread.endswith('/0'):
                    nodes.append(thread[-10:-2])

        # 2.2.3 Crash while receive unexpected message from command
        else:
            print("Warning: Unexpected messages occurred in 'qstat -n' message.")
            print("Please check the system")
            exit(1)
    
    if len(jobs_state) > 0:
        jobs_state[-1]['nodes'] = nodes

    return jobs_state

def pbsnodes_data_handler(pbs_msg):
    nodes = {}

    for line in pbs_msg.splitlines():
        if line.startswith('eureka'):
            node_name = line
            nodes[node_name] = {'state': 'unknown', 'jobs': []}
        elif ' state =' in line:
            nodes[node_name]['state'] = line.split()[2]
        elif ' jobs =' in line:
            jobs = line.split()[2:]
            jobs_info = {}
            for job in jobs:
                job_ID = job.split('.')[0].split('/')[1]
                if job_ID in jobs_info:
                    jobs_info[job_ID] += 1
                else:
                    jobs_info[job_ID] = 1
            nodes[node_name]['jobs'].append(jobs_info)
        else:
            continue

    return nodes


if __name__ == "__main__":
    # 1. Handle arguments
    args = arg_handler()

    # 2. Get node state : alive_or_dead user job_name job_ID Time
    node_state = get_node_state()

    # 3. Get node datas
    # 3.2 CPU : Usage and Temp.
    # 3.3 Memory usage
    # 3.4 GPU : Usage Mem_Usage Temp.
    # 3.5 IB : Status Speed
    # 3.6 Disk : Usage

    # 4. Merge data as a table

    # 5. Output
