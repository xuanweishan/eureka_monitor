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
import sys
import argparse
import threading

from os.path import isfile

sys.dont_write_bytecode = True

import src.run_cli as rc

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

def get_node_state():
    """
    Check alive nodes from pbs.

    Return:

    alive_nodes : list of alive nodes
    """
    # 1. Command lines to get the node stat data
    qstat = ['/usr/local/torque/bin/qstat']
    showq = ['/usr/local/maui/bin/showq']
    pbsnodes = ['/usr/local/torque/bin/pbsnodes', '-a']
    cmds = [qstat, pbsnodes, showq]
    
    # 2. Run cmds in multiple threads
    threads = []
    results = {}
    for cmd in cmds:
        thread = threading.Thread(target=rc.run_cli, args=(cmd, results))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    
    # 3. Deal with command results
    # 3.1 qstat
    job_stats = qstat_data_handler(results['qstat'])
    running_jobs = showq_data_handler(results['showq'])

    # 3.2 pbsnodes
    nodes_state = pbsnodes_data_handler(results['pbsnodes'])

    # 4. Merge messages of pbsnodes and qstat
    #    Using the structure of pbsnodes as skeleton
    return merge_pbs_qstat(nodes_state, job_stats, running_jobs)

def showq_data_handler(showq_msg):
    """
    Deal with message from command 'showq'

    Input:
        showq_msg : result string from the command

    Return:
        running_jobs : A dictionary with keys Job ID, for each element
                       record another dictionary with keys of 'Name', 'User', 'Time' 
    """

    # 1. Split message into lines
    lines = showq_msg.splitlines()
    # 2. grep active jobs
    running_jobs = {}
    for line in lines:
        if line.startswith('    '):
            break
        elif len(line) > 0 and line[0].isdigit():
            columns = line.split();
            jobID = columns[0]
            user = columns[1]
            state = columns[2]
            remain_time = columns[4]
            running_jobs[jobID] = {'User': user,
                                   'State': state,
                                   'Time': remain_time,
                                  }
    return running_jobs
    
def qstat_data_handler(qstat_msg):
    """
    Deal with message from command 'qstat'
    
    Input:
        qstat_msg : result string from the command

    Return:
        job_state : A dictionary of directiionaries to record job stat 
        {'Job_ID': { 'Name': ***, 'User', 'Time': **:**:**, 'Nodes': []}, ... }
    """
    # 1. Split total message into lines
    lines = qstat_msg.splitlines()[2:]
    
    # 2. Deal with message per line
    # 2.1 Initialization of variables required
    nodes = []
    jobs_state = {}

    # 2.2 Analyze message
    for line in lines:
        parts = line.split()
        # 2.2.1 Create new job info mation as read a new job.
        
        if len(parts) == 6:

            job_ID = parts[0].split('.')[0]
            name = parts[1]
            user = parts[2]
            time = parts[3]
            stat = parts[4]
            jobs_state[job_ID] = {
                                  'User': user,
                                  'Name': name,
                                  'Time': time,
                                  'Stat': stat,
                                 }
        # 2.2.2 Crash while receive unexpected message from command
        else:
            print("Warning: Unexpected messages occurred in 'qstat' message.")
            print("Please check the system")
            exit(1)
    
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

def merge_pbs_qstat(pbs, qstat, showq):
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
            if job in qstat :
                pbs[node]['Jobs'][job]['Name'] = qstat[job]['Name']
            else:
                pbs[node]['Jobs'][job]['Name'] = u'--'
            pbs[node]['Jobs'][job]['User'] = showq[job]['User']
            pbs[node]['Jobs'][job]['Time'] = showq[job]['Time']
    
    return pbs

def get_alive_nodes(node_state):
    """
    Grep alive nodes
    """
    alive_nodes = ['eureka00']
    for node in node_state:
        if node_state[node]['State'] in ['job-exclusive', 'free']:
            alive_nodes.append(node)

    return alive_nodes

def get_cpu_usage(alive_node):
    """
    Get CPU usage, temprature

    Input:
        alive_node : A list record alive nodes

    Return:
        cpu_usage : A dictionary with key of node names, save a dictionary with info of 'Usage' and 'Temp'
    """

    cmd = ['/usr/bin/mpstat', '1', '2']
    
    cmd_msg = {}
    cpu_usage = {}
    
    rc.run_pdsh_cli(cmd, alive_nodes, cmd_msg)

    lines = cmd_msg['mpstat'].splitlines()
    for line in lines:
        data = line.split()
        if len(data) > 2 and data[1] == u'Average:':
            cpu_usage[data[0][:-1]] = float(data[3])

    #results['cpu_usage'] = cpu_usage
    return cpu_usage

def get_cpu_temp(alive_node):
    cmd = ['/usr/bin/sensors']

    cmd_msg = {}
    cpu_temp = {}

    rc.run_pdsh_cli(cmd, alive_nodes, cmd_msg)

    lines = cmd_msg['sensors'].splitlines()
    for line in lines:
        data = line.split()
        if len(data)>1 and data[1] == 'temp1:':
            node_name = data[0][:-1]
            if node_name not in cpu_temp:
                cpu_temp[node_name] = float(data[2][1:-2]) / 2
            else:
                cpu_temp[node_name] += float(data[2][1:-2]) / 2


    for node in cpu_temp:
        cpu_temp[node] -= 27

    #results['cpu_temp'] = cpu_temp
    return cpu_temp

def get_memory_usage(alive_node):
    """
    """
    
    cmd = ['/usr/bin/free']

    cmd_msg = {}
    mem_usage = {}

    rc.run_pdsh_cli(cmd, alive_node, cmd_msg)

    lines = cmd_msg['free'].splitlines()
    for line in lines:
        data = line.split()
        if data[1] == 'Mem:':
            mem_usage[data[0][:-1]] = float(data[3]) / float(data[2]) * 100
    
    #results['Mem_usage'] = mem_usage
    return mem_usage

def get_gpu_usage(alive_node):
    """
    """

    cmd = ['/usr/bin/nvidia-smi', '-q']

    cmd_msg = {}
    gpu_usage = {}

    rc.run_pdsh_cli(cmd, alive_node, cmd_msg)

    lines = cmd_msg['nvidia-smi'].splitlines()
    for line in lines:
        data = line.split()
        node_name = data[0][:-1]
        if node_name not in gpu_usage:
            gpu_usage[node_name] = {}

        if 'Gpu' in line:
            gpu_usage[node_name]['Usage'] = float(data[-2])
        elif 'GPU Current Temp' in line:
            gpu_usage[node_name]['Temp'] = float(data[-2])
        elif 'Memory' in line and '%' in line:
            gpu_usage[node_name]['Mem'] = float(data[-2])
            
    #results['GPU_usage'] = gpu_usage
    return gpu_usage

def get_IB_speed(alive_node):
    """
    """
    cmd = ['/usr/sbin/iblinkinfo', '-l']
    if os.path.basename(os.path.expanduser('~')) != 'root':
        print("Info: Normal user cannot run the command %s %s" %(cmd[0], cmd[1]))
        return 0

    cmd_msg = {}
    IB_speed = {}

    rc.run_cli(cmd, cmd_msg)

    lines = cmd_msg['iblinkinfo'].splitlines()
    for line in lines:
        data = line.split()
        if data[2].startswith('eureka'):
            IB_speed[data[2]] = int(data[8][0]) * float(data[9])

    #results['IB_speed'] = IB_speed
    return IB_speed

def get_IB_adaptor_temp(alive_node):
    """
    """
    cmd = ['mget_temp', '-d', '/dev/mst/*']
    if os.path.basename(os.path.expanduser('~')) != 'root':
        print("Info: Normal user cannot run the command %s" % cmd[0])
        return 0
    
    cmd_msg = {}
    IB_temp = {}

    rc.run_pdsh_cli(cmd, alive_node, cmd_msg)

    lines = cmd_msg['mget_temp'].splitlines()
    for line in lines:
        data = line.split()
        IB_temp[data[0][:-1]] = data[1]

    #results['IB_temp'] = IB_temp
    return IB_temp

def get_disk_usage(alive_node):
    """
    """

    cmd = ['/usr/bin/df']

    cmd_msg = {}
    df_usage = {}

    rc.run_pdsh_cli(cmd, alive_node, cmd_msg)

    lines = cmd_msg['df'].splitlines()
    for line in lines:
        data = line.split()
        node_name = data[0][:-1]
        if data[-1] == '/':
            df_usage[node_name] = data[-2]
    
    #results['disk_usage'] = df_usage
    return df_usage

def merge_data(state, cpu, cpu_temp, mem, gpu, ib_speed, ib_temp, disk):
    
    for node in state:
        if 'down' in state[node]['State'] and node != 'eureka00': continue
        #cpu
        state[node]['CPU_usage'] = cpu[node]
        state[node]['CPU_temp']  = cpu_temp[node]
        #mem
        state[node]['Mem_usage'] = mem[node]
        #gpu
        state[node]['GPU_usage'] = gpu[node]['Usage']
        state[node]['GPU_mem']   = gpu[node]['Mem']
        state[node]['GPU_temp']  = gpu[node]['Temp']
        #ib_speed
        if not ib_speed == 0:
            state[node]['IB_speed']  = ib_speed[node]
        #ib_temp
        if not ib_temp == 0:
            state[node]['IB_temp']   = ib_temp[node]
        #disk
        state[node]['Disk_usage']= disk[node]

    return state

def output(data, mode):
    """
    """
    
    if mode == 'p':
        print_output(data)
            

    elif mode == 's':
        print('a')
    else:
        print("Error no such mode")
    
    return 0

def print_output(data):
    # First line define name for each column
    print("%-12s %-16s %-16s %-8s %8s %8s %8s %8s %8s %8s %8s %8s %8s"
         %('Node_name', 
           'User', 'Job_name', 'Job_ID',
           '%CPU', 'CPU_Mem',  'T_CPU',
           '%GPU', 'GPU_Mem',  'T_GPU',
           'IB_speed', 'T_IB',     'Disk',
         ))

    for node in sorted(data):
        count = 0
        if 'IB_speed' in data[node]:
            IB_speed = "%.3f" % data[node]['IB_speed']
        else:
            IB_speed = '--'

        if 'IB_temp' in data[node]:
            IB_temp = data[node]['IB_temp']
        else:
            IB_temp = '--'
        
        if 'down' in data[node]['State'] and node != 'eureka00':
            print("%-12s %-16s" %(node, 'Down'))
            continue

        elif len(data[node]['Jobs']) == 0:
            print("%-12s %-16s %-16s %-8s %8.1f %8.2f %8.1f %8.1f %8.2f %8.1f %8s %8s %8s"
                 %(node, 
                   '--','--', '--',
                   data[node]['CPU_usage'], data[node]['Mem_usage'], data[node]['CPU_temp'],
                   data[node]['GPU_usage'], data[node]['GPU_mem'], data[node]['GPU_temp'],
                   IB_speed, IB_temp, data[node]['Disk_usage'],
                 ))
            continue

        for jobID in data[node]['Jobs']:
            if count == 0:
                print("%-12s %-16s %-16s %-8s %8.1f %8.2f %8.1f %8.1f %8.2f %8.1f %8s %8s %8s"
                     %(node, 
                       data[node]['Jobs'][jobID]['User'], data[node]['Jobs'][jobID]['Name'], jobID,
                       data[node]['CPU_usage'], data[node]['Mem_usage'], data[node]['CPU_temp'],
                       data[node]['GPU_usage'], data[node]['GPU_mem'], data[node]['GPU_temp'],
                       IB_speed, IB_temp, data[node]['Disk_usage'],
                     ))
                count += 1
            else:
                print("%-12s %-16s %-16s %-8s %8s %8s %8s %8s %8s %8s %8s %8s %8s"
                     %('',
                       data[node]['Jobs'][jobID]['User'], data[node]['Jobs'][jobID]['Name'], jobID , data[node]['Jobs'][jobID]['Time'],
                       '','','','','','','','','',
                     ))


if __name__ == "__main__":
    # 1. Handle arguments
    args = arg_handler()
    if args.output == None:
        mode = 'p'
    else:
        mode = 's'

    # 2. Get node state : alive_or_dead user job_name job_ID Time
    node_state = get_node_state()
    alive_nodes = get_alive_nodes(node_state)

    # 3. Get node datas
    Threads = []
    results = {}
    # 3.1 CPU : Usage and Temp.
    
    cpu_usage = get_cpu_usage(alive_nodes)
    cpu_temp = get_cpu_temp(alive_nodes)
    
    # 3.2 Memory usage
    Mem_usage = get_memory_usage(alive_nodes)

    # 3.3 GPU : Usage Mem_Usage Temp.
    GPU_usage = get_gpu_usage(alive_nodes)

    # 3.4 IB : Status Speed
    IB_speed = get_IB_speed(alive_nodes)

    # 3.5 IB : IB adaptor Temp.
    IB_temp = get_IB_adaptor_temp(alive_nodes)

    # 3.6 Disk : Usage
    disk_usage = get_disk_usage(alive_nodes)

    # 4. Merge data as a table
    output_data = merge_data(node_state, 
                             cpu_usage, cpu_temp, Mem_usage,
                             GPU_usage, IB_speed, IB_temp,
                             disk_usage,
                            )

    # 5. Output
    output(output_data, mode)
    
