import socket
import struct

fmt = struct.Struct('!cii')

def msg(a, b, c):
    return fmt.pack(a.encode('ascii'), b, c)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(("localhost", 9999))
    sock.sendall(msg('I', 12345, 101))
    sock.sendall(msg('I', 12346, 102))
    sock.sendall(msg('I', 12347, 100))
    sock.sendall(msg('I', 40960, 5))
    sock.sendall(msg('Q', 12288, 16384))
    print (struct.unpack('!i', sock.recv(4)))
    sock.sendall(b'\0')
    print(sock.recv(10))
    sock.close()
