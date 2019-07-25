from __future__ import print_function
#!/usr/bin/env python
'''
Jupyter_Bsub - Luca Pinello & Kendell Clement 2017
Connect to a LSF main node directly or trough a ssh jump node, launch a jupyter notebook via bsub and open automatically a tunnel.
'''
__version__ = "0.3.0"


import subprocess as sb
import time
import re
import os
import sys
from random import randint
import argparse
import socket


def hostname_resolves(hostname):
    try:
        socket.gethostbyname(hostname)
        return True
    except socket.error:
        return False


def query_yes_no(question, default="yes"):
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")

def open_connect(ssh_cmd, server, bsub_args, connection_filename,
                 localport, remoteport, env='', debug=False):
    #launch a job
    if args.env:
    	env_cmd = 'source activate {0} &&'.format(args.env)
    else:
    	env_cmd = ''
    cmd_ssh = '{ssh:s} -t {server:s}'.format(ssh=ssh_cmd, server=server)
    cmd_bsub = ('''bsub -q {queue:s} -n {n_cores:d} -M {memory:d} '''
                '''-cwd {remote_path:s} -R 'rusage[mem={memory:d}]' ''').format(
                    **bsub_args)
    cmd_jupyter = '''jupyter notebook --port={:d} --no-browser'''.format(
        remoteport)
    cmd_redirect = "2>&1 > {:s}".format(connection_filename)
    cmd = '''{ssh} "{bsub} '{env} {jupyter}' {redirect}" 2>/dev/null'''
    cmd = cmd.format(ssh=cmd_ssh, bsub=cmd_bsub, env=env_cmd,
                     jupyter=cmd_jupyter, redirect=cmd_redirect)
    if debug: print(cmd_jupyter)
    sb.call( cmd_jupyter,shell=True)
    cmd_file_write = '{ssh} "echo {local:d},{remote:d} >> {fn:s}" 2> /dev/null'
    cmd_file_write = cmd_file_write.format(ssh=cmd_ssh, local=localport,
        remote=remoteport, fn=connection_filename)
    if debug: print(cmd_file_write)
    sb.call(cmd_file_write,shell=True)
    connection_status = True

print('''
 _               _           _                   _
| |__  ___ _   _| |__       (_)_   _ _ __  _   _| |_ ___ _ __
| '_ \/ __| | | | '_ \      | | | | | '_ \| | | | __/ _ \ '__|
| |_) \__ \ |_| | |_) |     | | |_| | |_) | |_| | ||  __/ |
|_.__/|___/\__,_|_.__/____ _/ |\__,_| .__/ \__, |\__\___|_|
                    |_____|__/      |_|    |___/

''')
print('\n\n[Luca Pinello 2017, send bugs, suggestions or *green coffee* to lucapinello AT gmail DOT com]\n\n')
print( 'Version %s\n' % __version__)

parser = argparse.ArgumentParser(description='bsub_jupyter\n\n- Connect to a LSF main node directly or trough a ssh jump node, launch a jupyter notebook via bsub and open automatically a tunnel.',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('lsf_server', type=str,  help='username@server, the server is the main LSF node used to submit jobs with bsub')
parser.add_argument('connection_name', type=str,  help='Name of the connection')


#OPTIONALS
parser.add_argument('--remote_path', type=str,  help='remote path to use',default='~')
parser.add_argument('--bastion_server',  help='SSH jump server, format username@server', default=None)
parser.add_argument('--memory', type=int,  help='Memory to request', default=64000)
parser.add_argument('--n_cores', type=int,  help='# of cores to request', default=8)
parser.add_argument('--queue', type=str,  help='Queue to submit job',default='big-multi')
parser.add_argument('--force_new_connection',  help='Ignore any existing connection file and start a new connection', action='store_true')
parser.add_argument('--ignoreHostChecking',  help='Ignore known host checking. If your client-side tunnel is not created and you get a message starting "The authenticity of host {xxx} can\'t be established." try enabling this flag.', action='store_true')
parser.add_argument('--debug',  help='Print helpful debug messages', action='store_true')
parser.add_argument('--env', type=str, help='load a different env for python')

args = parser.parse_args(['fultob01@minerva.hpc.mssm.edu', 'remote_jupyter'])

username, hostname_server = args.lsf_server.split('@')


ssh_server = args.lsf_server
bastion_server = args.bastion_server

local_bastion_port = 10001
ssh_port = 22

if bastion_server:
    if not hostname_resolves(bastion_server):
        print('Cannot resolve bastion server %s. Check server name and try again.' % bastion_server)
        sys.exit(1)

    #ssh  -L 9000:eris1n2.research.partners.org:22 lp698@ssh.research.partners.org

    #create tunnel via bastion server
    cmd_bastion_tunnel = 'ssh -N -f -L {0}:{1}:{2} {3} '.format(local_bastion_port,hostname_server,ssh_port,bastion_server)
    if args.debug : print(cmd_bastion_tunnel)
    sb.call(cmd_bastion_tunnel,shell=True)

    ssh_server = " {0}@{1} -p {2} ".format(username,"localhost", local_bastion_port)

base_ssh_cmd = "ssh "

assert hostname_resolves(hostname_server), 'Cannot resolve %s. Check server name and try again.' % hostname_server

connection_name = args.connection_name
connection_filename = 'jupyter_connection_%s' % connection_name

bsub_args = {'queue': args.queue, 'n_cores': args.n_cores,
             'memory': args.memory, 'remote_path': args.remote_path}

random_local_port = randint(9000,10000)
random_remote_port = randint(9000,10000)

print('Checking if a connection alrady exists...')
#check if the connection  exists already
connect_check = '{ssh:s} -t {server:s} "[ -f {connect:s} ] '.format(
    ssh=base_ssh_cmd, server=ssh_server, connect=connection_filename)
connect_check += '&& echo True || echo False" 2> /dev/null'
connection_status = sb.check_output(connect_check, shell=True).strip().decode()

if connection_status == 'True' and not args.force_new_connection:
    print('A running job already exists!')
else:
	print('No running jobs were found, launching a new one! ')
    open_connect(base_ssh_cmd, ssh_server, bsub_args,
        connection_filename, random_local_port, random_remote_port)

job_id = sb.check_output('%s %s " head -n 1 ~/%s" 2> /dev/null' % (base_ssh_cmd,ssh_server,connection_filename),shell=True).split('<')[1].split('>')[0]
random_local_port, random_remote_port = map(int,sb.check_output('%s %s "tail -n 1 ~/%s" 2> /dev/null' % (base_ssh_cmd,ssh_server,connection_filename),shell=True).strip().split(','))

print('JOB ID:',job_id)

if  connection_status=='True':
    if query_yes_no('Should I kill it?'):
        bkill_command = '%s -t %s "bkill %s; rm %s" 2> /dev/null' % (base_ssh_cmd,ssh_server,job_id,connection_filename)
        sb.call(bkill_command,shell=True)
        sys.exit(0)

# use bjobs to get the node the server is running on
server = None
print('Querying queue for job info..')
while server is None:

    bjob_command = '%s -t %s "bjobs -l %s" 2> /dev/null' % (base_ssh_cmd,ssh_server,job_id)
    if args.debug: print("bjob_command: " + bjob_command)
    p = sb.Popen(bjob_command, stdout=sb.PIPE, stderr=sb.PIPE,shell=True)
    out, err = p.communicate()

    print('.',end = "")
    sys.stdout.flush()

    m = re.search('<(.*)>, Execution Home', out)

    try:
        server =  m.groups()[0].split('*')[-1]
    except AttributeError:
        time.sleep(1)

print('\nServer launched on node: '+server)

print('Local port: %d  remote port: %d' %(random_local_port, random_remote_port))

if sb.check_output("nc -z localhost %d || echo 'no tunnel open';" % random_local_port,shell=True).strip()=='no tunnel open':
    if query_yes_no('Should I open an ssh tunnel for you?'):
        sb.call('sleep 5 && python -m webbrowser -t "http://localhost:%d" & 2> /dev/null' % random_local_port,shell=True)
        tunnel_ssh_command = "ssh "
        if args.ignoreHostChecking: tunnel_ssh_command = "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "
        cmd_tunnel = tunnel_ssh_command + " -N  -L localhost:{0}:localhost:{1} -o 'ProxyCommand ssh {2} nc %h %p'  {3}@{4}.research.partners.org 2> /dev/null".format(random_local_port,random_remote_port,ssh_server,username,server)
        if args.debug: print(cmd_tunnel)
        try:
            print('Tunnel created! You can see your jupyter notebook server at:\n\n\t--> http://localhost:%d <--\n' % random_local_port)
            print('Press Ctrl-c to interrupt the connection')
            sb.call(cmd_tunnel,shell=True)
        except:
            print('Tunnel closed!')
            if query_yes_no('Should I kill also the job?'):
                bkill_command = '%s -t %s "bkill %s; rm %s" 2> /dev/null' % (base_ssh_cmd,ssh_server,job_id,connection_filename)
                sb.call(bkill_command,shell = True)
                sys.exit(0)
else:
    print('Tunnel already exists!')
