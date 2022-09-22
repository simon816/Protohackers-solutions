import socket
import socketserver

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        sock = self.request
        sock.settimeout(1)
        data = b''
        while True:
            print("Get buf")
            try:
                buf = sock.recv(1024)
            except socket.timeout:
                break
            data += buf
            if not buf:
                break
        print("Recv", data)
        sock.sendall(data)
        sock.close()
        print("Done")

socketserver.TCPServer(('0.0.0.0', 9999), Handler).serve_forever()
