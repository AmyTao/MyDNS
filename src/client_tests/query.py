import socket
import threading
import json 

def receive_messages(sock):
    while True:
        data, addr = sock.recvfrom(1024)  # buffer size is 1024 bytes
        print(f"Received message from {addr}: {data.decode()}")

def send_messages(sock):
    message = json.dumps({"Type": "Query", "Key": "future", "Value": None})
    sock.sendto(message.encode(), ('0.0.0.0', 9877))

    message = json.dumps({"Type": "Query", "Key": "future1", "Value": None})
    sock.sendto(message.encode(), ('0.0.0.0', 9877))

    message = json.dumps({"Type": "Query", "Key": "future2", "Value": None})
    sock.sendto(message.encode(), ('0.0.0.0', 9876))

    message = json.dumps({"Type": "Query", "Key": "future3", "Value": None})
    sock.sendto(message.encode(), ('0.0.0.0', 9876))


def main():
    # 创建UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', 12345))

    # 创建并启动接收消息的线程
    receiver_thread = threading.Thread(target=receive_messages, args=(sock,))
    receiver_thread.start()

    # 创建并启动发送消息的线程
    sender_thread = threading.Thread(target=send_messages, args=(sock,))
    sender_thread.start()

    sender_thread.join()
    receiver_thread.join()
    sock.close()
    print("Socket closed, program terminated.")

if __name__ == "__main__":
    main()
