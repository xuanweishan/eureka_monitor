import os
import subprocess



def run_cli(cmd, results):
    """
    Run the command line and catch the return message.

    Input:
        cmd : A list that record command going to run.
        results : A dictionary with key of command name and the result message.
    """

    # 1. Execute command with subprocess.
    try:
        result = subprocess.check_output(cmd)
        results[os.path.basename(cmd[0])] = result.decode('utf-8').strip()
    # 2. Print out the error message and exit the program.
    except subprocess.CalledProcessError as err:
        print("Error: error occurred while running %s." %(cmd))
        print("    Error code: ", err.returncode)
        print("    Fail message:")
        print(err.output.decode('utf-8'))
        exit(1)



def run_pdsh_cli(cmd, nodes, results):
    """
    Run the command line with pdsh and catch the return message.

    Input:
        cmd : A list that record command going to run.
        nodes: A list that record nodes we are going to run command on.
        results : A dictionary with key of command name and the result message.
    """
    # 1. Join the pdsh command and commands.
    pdsh_cmd = ['pdsh', '-u', '10', '-w', ','.join(nodes)] + cmd
    # 2. Execute command with subprocess.
    try:
        result = subprocess.check_output(pdsh_cmd)
        results[os.path.basename(cmd[0])] = result.decode('utf-8').strip()
    # 3. Print out the error message and exit the program.
    except subprocess.CalledProcessError as err:
        print("Error: error occurred while running %s." %(pdsh_cmd))
        print("    Error code: ", err.returncode)
        print("    Fail message:")
        print(err.output.decode('utf-8'))
        exit(1)


# Self function test
if __name__ == '__main__':
   cmd = ['echo', 'hello']
   results = {}
   run_cli(cmd,results)
   print(results)
   
   run_pdsh_cli(cmd,['eureka00,eureka01'],results)
   print(results)

