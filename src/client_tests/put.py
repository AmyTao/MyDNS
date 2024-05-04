import socket
import json
import time

def send_udp_message(ip, port, message):
    # 创建socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # 指定的服务器地址和端口
    server_address = (ip, port)
    
    try:
        # 发送消息
        sent = sock.sendto(message.encode(), server_address)
        print(f"Sent {sent} bytes to {ip}:{port}")
    finally:
        # 关闭socket
        sock.close()

if __name__ == "__main__":
    ip = "127.0.0.1"
    port = 9876
    message = json.dumps({"Type": "Put", "Key": "future", "Value": "10.23.64.10"})
    send_udp_message(ip, port, message)

    time.sleep(0.1)

    message = json.dumps({"Type": "Put", "Key": "future1", "Value": "10.23.64.11"})
    send_udp_message(ip, port, message)

    time.sleep(0.1)

    message = json.dumps({"Type": "Put", "Key": "future2", "Value": "10.23.64.12"})
    send_udp_message(ip, port, message)