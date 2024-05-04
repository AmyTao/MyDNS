import socket
import threading
import json
import random
import time
import string
import argparse

def receive_messages(sock):
    while True:
        data, addr = sock.recvfrom(1024)  # buffer size is 1024 bytes
        print(f"Received message from {addr}: {data.decode()}")

def generate_random_key(length=10):
    """Generate a random string of fixed length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_ip():
    """Generate a random IP address within a specified subnet."""
    return f"10.23.64.{random.randint(1, 254)}"

def send_messages(sock, base_port, nclients):
    types = ["Query", "Put"]
    base_ip = '127.0.0.1'
    
    send_put_counter = 0
    send_query_counter = 0
    while True:
        message_type = random.choice(types)
        if message_type == "Put":
            send_put_counter += 1
        else:
            send_query_counter += 1
        # print(f"Total Sent {send_put_counter} PUTs and {send_query_counter} QUERYs")
        key = generate_random_key()
        value = generate_ip() if message_type == "Put" else None
        target_port = base_port + random.randint(0, nclients - 1)
        message = json.dumps({"Type": message_type, "Key": key, "Value": value})
        sock.sendto(message.encode(), (base_ip, target_port))
        # time.sleep(random.uniform(0.1, 0.4))  # Random delay between messages

def main():
    parser = argparse.ArgumentParser(description='Flood test for the key-value store.')
    parser.add_argument('--nclients', type=int, default=10, help='Number of client ports')

    args = parser.parse_args()

    # Configuration
    base_port = 9876
    nclients = args.nclients  # Number of client ports
    local_port = 12345

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('127.0.0.1', local_port))

    # Start the thread for receiving messages
    receiver_thread = threading.Thread(target=receive_messages, args=(sock,))
    receiver_thread.daemon = True
    receiver_thread.start()

    # Start the thread for sending messages
    sender_thread = threading.Thread(target=send_messages, args=(sock, base_port, nclients))
    sender_thread.daemon = True
    sender_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program terminated by user.")
    finally:
        sock.close()
        print("Socket closed.")

if __name__ == "__main__":
    main()
