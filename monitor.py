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
        results[os.path.basename(cmd[0])] = result.decode('utf-8').strip()
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
    qstat = ['/usr/local/torque/bin/qstat', '-n']
    pbsnodes = ['/usr/local/torque/bin/pbsnodes', '-a']
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

    # 3.2 pbsnodes
    nodes_state = pbsnodes_data_handler(results['pbsnodes'])

    # 4. Merge messages of pbsnodes and qstat
    #    Using the structure of pbsnodes as skeleton
    return merge_pbs_qstat(nodes_state, job_stats)

    
def qstat_data_handler(qstat_msg):
    """
    Deal with message from command 'qstat -n'
    
    input:

    qstat_msg : result string from the command

    Return:

    job_state : A list of directiionaries to record job stat 
    [{'Job_ID': ***, 'Name': ***, 'User', 'Time': **:**:**, 'Nodes': []}, ]
    """
    # 1. Split total message into lines
    lines = qstat_msg.splitlines()[4:]
    
    # 2. Deal with message per line
    # 2.1 Initialization of variables required
    nodes = []
    jobs_state = {}

    # 2.2 Analyze message
    for line in lines:
        parts = line.split()
        # 2.2.1 Create new job info mation as read a new job.
        
        if len(parts) == 11:
            # 2.2.1.1 Add using nodes to previous job state
            if len(jobs_state) > 0:
                jobs_state[job_ID]['Nodes'] = nodes

            # 2.2.1.2 Create a new job state
            job_ID = parts[0].split('.')[0]
            user = parts[1]
            name = parts[3]
            stat = parts[9]
            time = parts[10]
            nodes = []
            jobs_state[job_ID] = {
                                  'User': user,
                                  'Name': name,
                                  'Time': time,
                                  'Stat': stat,
                                 }
        
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
        jobs_state[job_ID]['Nodes'] = nodes

    return jobs_state

def pbsnodes_data_handler(pbs_msg):
    """
    Deal with the output message of command "pbsnodes".

    input:
        pbs_msg : a srting from command "pbsnodes".
    
    output:
        nodes : a dictionary with keys of node names, 
                for each node is also a dictionary store keys of 'state', 'jobs'
                'jobs' is also a dictionary with keys of job IDs, store the threads on that node.
        nodes = {
                'eurekaXX': {'state': 'xx', 'jobs': {'ID1': xxxx, 'ID2': xxxx}},
                }
    """
    
    # 1. Initialize the data will be return
    nodes = {}

    # 2. Grep needed state data line by line
    for line in pbs_msg.splitlines():
        
        # 2.1 Get node name and initial node state dict
        if line.startswith('eureka'):
            node_name = line
            nodes[node_name] = {'State': 'unknown', 'Jobs': {}}
        
        # 2.2 Get node state
        elif ' state =' in line:
            nodes[node_name]['State'] = line.split()[2]
        
        # 2.3 Get jobs on the node
        elif ' jobs =' in line:
            jobs = line.split()[2:]
            jobs_info = {}
            for job in jobs:
                job_ID = job.split('.')[0].split('/')[1]
                if job_ID in jobs_info:
                    jobs_info[job_ID]['np'] += 1
                else:
                    jobs_info[job_ID] = {'np': 1}
            nodes[node_name]['Jobs'] = jobs_info
        else:
            continue

    return nodes

def merge_pbs_qstat(pbs, qstat):
    """
    Merge the messages from commane "pbsnodes" and "qstat"

    Input:

        pbs: A dictionary recording the node state from "pbsnodes"
             Will be used as the skeleton of return data
        
        qstat: A list recording the jobs info.

    Return:

        nodes_state: 
            A dictionary recording the stats and jobs info on each node.

    """
    # 1. Arrange messages node by node.

    for node in pbs:
        for job in pbs[node]['Jobs']:
            if job not in qstat:
                continue
            pbs[node]['Jobs'][job]['Name'] = qstat[job]['Name']
            pbs[node]['Jobs'][job]['User'] = qstat[job]['User']
            pbs[node]['Jobs'][job]['Time'] = qstat[job]['Time']
    
    return pbs

def get_alive_nodes(node_state):
    alive_nodes = []
    for node in node_state:
        if node_state[node]['State'] in ['job-exclusive', 'free']:
            alive_nodes.append(node)

    return alive_nodes

def get_cpu_info(alive_node):
    """
    Get CPU usage, temprature

    Input:
        alive_node : A list record alive nodes

    Return:
        cpu_usage : A dictionary with key of node names, save a dictionary with info of 'Usage' and 'Temp'
    """

    cmds = {
            'usage' : ['/usr/bin/mpstat'],
            'temp' : ['/usr/bin/sensors'],
           }
    return 0

def get_memory_usage(alive_node):
    """
    """
    
    cmd = ['/usr/bin/free']
    return 0

def get_gpu_usage(alive_node):
    """
    """

    cmd = ['/usr/bin/nvidia-smi', '-q']
    return 0

def get_IB_speed(alive_node):
    """
    """

    cmd = ['/usr/sbin/iblinkinfo', '-l']
    return 0

def get_disk_usage(alive_node):
    """
    """

    cmd = ['/usr/bin/df']
    return 0

def merge_data(state, cpu, mem, gpu, ib, disk):
    return 0

def output(data):
    return 0

if __name__ == "__main__":
    # 1. Handle arguments
    args = arg_handler()

    # 2. Get node state : alive_or_dead user job_name job_ID Time
    node_state = get_node_state()
    print(node_state)
    alive_nodes = get_alive_nodes(node_state)
    print(alive_nodes)

    # 3. Get node datas
    
    # 3.1 CPU : Usage and Temp.
    cpu_usage = get_cpu_info(alive_nodes)
    
    # 3.2 Memory usage
    mem_usage = get_memory_usage(alive_nodes)

    # 3.3 GPU : Usage Mem_Usage Temp.
    gpu_usage = get_gpu_usage(alive_nodes)

    # 3.4 IB : Status Speed
    IB_speed = get_IB_speed(alive_nodes)

    # 3.5 Disk : Usage
    disk_usage = get_disk_usage(alive_nodes)

    # 4. Merge data as a table
    output_data = merge_data(node_state, cpu_usage, mem_usage, gpu_usage, IB_speed, disk_usage)

    # 5. Output
    output(output_data)
    
