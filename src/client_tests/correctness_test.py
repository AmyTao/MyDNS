import socket
import threading
import json
import random
import time
import string
import sys

ip_pairs_local = {}

ip_pairs_remote = {}

mutex = threading.Lock()

def receive_messages(sock, expected_messages, done_event):
    received_count = 0
    while received_count < expected_messages:
        data, addr = sock.recvfrom(1024)  
        print(f"Received message from {addr}: {data.decode()}")
        packet = json.loads(data.decode())
        with mutex:
            ip_pairs_remote[packet["Key"]] = packet["Value"]
        received_count += 1
    done_event.set()  # Signal that all messages have been received

def generate_random_key(length=10):
    """Generate a random string of fixed length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_ip():
    """Generate a random IP address within a specified subnet."""
    return f"10.23.64.{random.randint(1, 254)}"

def send_put(sock, key, value, base_ip, target_port):
    message = json.dumps({"Type": "Put", "Key": key, "Value": value})
    sock.sendto(message.encode(), (base_ip, target_port))

def send_query(sock, key, base_ip, target_port):
    message = json.dumps({"Type": "Query", "Key": key})
    sock.sendto(message.encode(), (base_ip, target_port))

def worker_put(sock, base_port, target_port, key_value_pairs):
    base_ip = '0.0.0.0'
    for key, value in key_value_pairs:
        send_put(sock, key, value, base_ip, target_port)

def worker_query(sock, base_port, nclients, all_keys):
    base_ip = '0.0.0.0'
    target_ports = [base_port + i for i in range(nclients)]
    for key in all_keys:
        target_port = random.choice(target_ports)  # Randomly pick a target port for load balancing
        send_query(sock, key, base_ip, target_port)

def main():
    # Configuration
    base_port = 9876
    nclients = 5  # Number of client ports
    local_port = 12345
    num_operations = 5  # Number of put/query operations per client
    total_messages = nclients * num_operations  # Total query messages expected (only counts queries)

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', local_port))

    # Event to signal when all responses are received
    done_event = threading.Event()

    # Start the thread for receiving messages
    receiver_thread = threading.Thread(target=receive_messages, args=(sock, total_messages, done_event))
    receiver_thread.daemon = True
    receiver_thread.start()

    # Generate unique key-value pairs for each client
    clients_pairs = {i: [(generate_random_key(), generate_ip()) for _ in range(num_operations)] for i in range(nclients)}
    all_keys = [key for sublist in clients_pairs.values() for key, _ in sublist]
    ip_pairs_local.update({key: value for sublist in clients_pairs.values() for key, value in sublist})

    # Start threads for PUT requests
    for i in range(nclients):
        target_port = base_port + i
        threading.Thread(target=worker_put, args=(sock, base_port, target_port, clients_pairs[i])).start()

    # Ensure all PUTs complete before starting queries
    time.sleep(2)  # Wait for all PUT operations to complete; adjust as necessary

    # Start threads for QUERY requests
    threading.Thread(target=worker_query, args=(sock, base_port, nclients, all_keys)).start()

    # Wait for all messages to be received
    done_event.wait()
    print("All expected messages received. Closing socket.")

    print("The local address pairs and remote address pairs is same:", ip_pairs_local == ip_pairs_remote)

    sock.close()
    sys.exit(0)  # Exit the program cleanly

if __name__ == "__main__":
    main()
