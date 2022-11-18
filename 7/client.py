import socket
import struct
import time

addr = ('localhost', 9999)

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.sendto(b'/connect/1/', addr)
    time.sleep(.5)
    print(sock.recvfrom(8192))
    #sock.sendto(b'/data/1/0/foo\\/bar\\/baz\nfoo\\\\bar\\\\baz\n/', addr)
    sock.sendto(b'/data/1/0/qwerty/', addr)
    time.sleep(.5)
    print(sock.recvfrom(8192))
    sock.sendto(b'/data/1/1/werty\nuiop/', addr)
    time.sleep(.5)
    print(sock.recvfrom(8192))
    sock.sendto(b'/data/1/11/\n/', addr)
    time.sleep(.5)
    print(sock.recvfrom(8192))
