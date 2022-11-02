import socket
import struct
import time

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(("localhost", 9999))
    sock.sendall(struct.pack('!BHHH', 0x80, 1, 1, 1))
    sock.sendall(struct.pack('!BHHH', 0x80, 1, 1, 1))

    time.sleep(1)

    msg_id = sock.recv(1)[0]
    time.sleep(1)
    print(msg_id)
    if msg_id == 0x10:
        l = sock.recv(1)[0]
        print(sock.recv(l))
