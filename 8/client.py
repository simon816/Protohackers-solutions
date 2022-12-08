import socket
import struct
import time

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(("localhost", 9999))
    sock.sendall(bytes.fromhex('02 7b 05 01 00'))

    sock.sendall(bytes.fromhex('f2 20 ba 44 18 84 ba aa d0 26 44 a4 a8 7e'))

    print(sock.recv(100).hex(' '))

    sock.sendall(bytes.fromhex('6a 48 d6 58 34 44 d6 7a 98 4e 0c cc 94 31'))

    print(sock.recv(100).hex(' '))
