import socket
import struct
import time

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(("localhost", 9999))
    print("Send WantHeartbeat")
    sock.sendall(struct.pack('!BI', 0x40, 5))
    while True:
        print(time.time(), "Recv:", sock.recv(1))
