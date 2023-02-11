import socket
import struct
import time

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(("localhost", 9999))
    sock.sendall(bytes.fromhex('50 00 00 00 19 00 00 00 0b 70 65 73 74 63 6f 6e 74 72 6f 6c 00 00 00 01 ce'))
    print(sock.recv(25))

