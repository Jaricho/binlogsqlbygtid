#!/usr/bin/env python
#-*- encoding: utf8 -*-

import sys
import argparse
import getpass
import paramiko

def parse_args():
    """parse args for binloginfo gtid"""

    parser = argparse.ArgumentParser(description='Get MySQL Binlog info by GTID', add_help=False)
    connect_params = parser.add_argument_group('connect params')
    connect_params.add_argument('-h', '--host', dest='host', type=str, help='MySQL Server Host', default='127.0.0.1')
    connect_params.add_argument('-P', '--port', dest='port', type=int, help='MySQL Server Host Port', default=3306)
    connect_params.add_argument('-u', '--user', dest='user', type=str, help='MySQL User Loginame', default='root')
    connect_params.add_argument('-p', '--password', dest='password', type=str, help='MySQL User Password', nargs='*', default='')
    parser.add_argument('--server_user', dest='server_user', type=str, help='MySQL Machine user name', default='root')
    parser.add_argument('--server_password', dest='server_password', type=str, help='MySQL Machine user password', nargs='*', default='')
    parser.add_argument('--server_uuid', dest='server_uuid', type=str, help='MySQL Instance Server UUID', default='')
    parser.add_argument('--transno', dest='transno', type=str, help="MySQL Instance GTID transaction no", default='')
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    return parser

def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)

    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.server_uuid:
        raise ValueError('Please input server uuid')
    if not args.transno:
        raise ValueError('Please GTID transaction no')
    if not args.password:
        args.password = getpass.getpass(prompt='MySQL Login Password:')
    else:
        args.password = args.password[0]
    if not args.server_password:
        args.server_password = getpass.getpass(prompt='Server Login Password:')
    else:
        args.server_password = args.server_password[0]
    return args

def ssh_outs(ip, cmd, user='', password=''):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, port=1022, username=user, password=password, timeout=200)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    output = stdout.readlines()
    print stdout
    print stderr
    return output
    ssh.close()


