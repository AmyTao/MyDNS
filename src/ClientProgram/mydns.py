'''
The client program for the `MyDNS` Project.

'''

import socket
import json
import time
import argparse
from typing import List


SERVER_ADDRESS = ['10.23.64.10:9876', '10.23.64.10:9877', '10.23.64.10:9878']


def _color_print(text: str, color: str) -> None:
    ''' Print the text in the specified color. '''
    colors = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'purple': '\033[95m',
        'cyan': '\033[96m',
        'white': '\033[97m',
        'reset': '\033[0m'
    }
    print(f'{colors[color]}{text}{colors["reset"]}')


def _send_packet(sock: socket.socket, packet: dict, server_address: str) -> None:
    server_address = server_address.split(':')
    server_address = (server_address[0], int(server_address[1]))
    packet = json.dumps(packet).encode('utf-8')
    sock.sendto(packet, server_address)


def _receive_packet(sock: socket.socket, timeout: int) -> dict:
    sock.settimeout(timeout)
    try:
        packet, address = sock.recvfrom(1024)
        packet = json.loads(packet.decode('utf-8'))
    except socket.timeout:
        return None
    except json.JSONDecodeError:
        return None
    return packet


def _get_self_hostname() -> str:
    ''' Get the hostname of the current machine. '''
    return socket.gethostname()


def _get_self_ip_address() -> str:
    ''' Get the ip address of the current machine. '''
    return socket.gethostbyname(_get_self_hostname())


def send_receive(sock: socket.socket, packet: str, args: argparse.Namespace):
    ''' Send a packet to the server, and wait for a response. If timeout, use the next server address. '''
    for server_address in args.servers:
        _send_packet(sock, packet, server_address)
        response = _receive_packet(sock, args.timeout)
        if response is not None:
            return response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MyDNS Client Program")
    # select the action to perform: query or update
    subparser = parser.add_subparsers(dest='mode', required=True, help='Select the action to perform')

    parser_update = subparser.add_parser('update', help='Update the current ip address')
    parser_update.add_argument('--update_interval', type=int, default=60, help='The interval in seconds to update the current ip address')
    parser_update.add_argument('--self-ip', type=str, default=_get_self_ip_address(), help='The ip address of the current machine')
    parser_update.add_argument('--self-hostname', type=str, default=_get_self_hostname(), help='The hostname of the current machine')
    parser_update.add_argument('--servers', type=str, nargs='+', default=SERVER_ADDRESS, help='The DNS servers to put for the ip address')

    parser_query = subparser.add_parser('query', help='Query the ip address of a hostname')
    parser_query.add_argument('hostname', type=str, nargs='+', help='The hostname to query for the ip address')
    parser_query.add_argument('--servers', type=str, nargs='+', default=SERVER_ADDRESS, help='The DNS servers to query for the ip address')
    parser_query.add_argument('--timeout', type=int, default=2, help='The timeout in seconds to wait for a response')

    parser_dnsip = subparser.add_parser('dnsip', help='Get the ip address of the DNS servers')
    parser_dnsip.add_argument('--servers', type=str, nargs='+', default=SERVER_ADDRESS, help='The DNS servers to query for the ip address')
    parser_dnsip.add_argument('--num-servers', type=int, default=5, help='The guess number of DNS servers')
    parser_dnsip.add_argument('--timeout', type=int, default=2, help='The timeout in seconds to wait for a response')

    return parser.parse_args()


def main():
    args = parse_args()

    print('-' * 30)

    if args.mode == 'update':
        # In this mode, the client will update the current ip address to the DNS server
        print(f'Updating the current hostname: {args.self_hostname}, ip address: {args.self_ip}')
        while True:
            packet = {
                'Type': 'Put',
                'Key': args.self_hostname,
                'Value': args.self_ip
            }
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                for server_address in args.servers:
                    _send_packet(sock, packet, server_address)
            time.sleep(args.update_interval)

    elif args.mode == 'query':
        # In this mode, the client will query the ip address of a hostname from the DNS server
        for hostname in args.hostname:
            print(f'Querying for hostname: {hostname}...', end=' ')
            packet = {
                'Type': 'Query',
                'Key': hostname,
                'Value': None
            }
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                response = send_receive(sock, packet, args)
                if response is not None:
                    if response["Value"] == "":
                        _color_print(f'Failed!', 'red')
                    else:
                        _color_print(f'{response["Value"]}', 'green')
                else:
                    _color_print(f'Failed!', 'red')
    
    elif args.mode == 'dnsip':
        # In this mode, the client will ask for the DNS server ip addresses
        # DNS servers have reserved hostname "::Clerk<id>".
        dns_ip_list = []
        for i in range(args.num_servers):
            hostname = f'::Clerk{i}'
            print(f'Querying for hostname: {hostname}...', end=' ')
            packet = {
                'Type': 'Query',
                'Key': hostname,
                'Value': None
            }
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                response = send_receive(sock, packet, args)
                if response is not None:
                    if response["Value"] == "":
                        _color_print(f'Failed!', 'red')
                    else:
                        _color_print(f'Done!', 'green')
                        dns_ip_list.append(response['Value'])
                else:
                    _color_print(f'Failed!', 'red')
        # also tried to explore more, until cannot find
        done = False
        while not done:
            i += 1
            hostname = f'::Clerk{i}'
            print(f'Querying for hostname: {hostname}...', end=' ')
            packet = {
                'Type': 'Query',
                'Key': hostname,
                'Value': None
            }
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                response = send_receive(sock, packet, args)
                if response is not None:
                    if response["Value"] == "":
                        _color_print(f'Failed!', 'red')
                        done = True
                    else:
                        _color_print(f'Done!', 'green')
                        dns_ip_list.append(response["Value"])
                else:
                    _color_print(f'Failed!', 'red')
                    done = False
        _color_print('DNS server ip available:', 'blue')
        print('"', ' '.join(dns_ip_list), '"', sep='')
          
    else:
        raise ValueError('Invalid mode')

    print('-' * 30)


if __name__ == '__main__':
    main()
