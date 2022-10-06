import socketserver

store = {
    b'version': b'UDP Store 0.1',
}

class Handler(socketserver.BaseRequestHandler):
    
    def handle(self):
        data, sock = self.request
        spl = data.split(b'=', 1)
        if len(spl) == 1:
            key = data
            value = store.get(key, b'')
            sock.sendto(key + b'=' + value, self.client_address)
        else:
            key, value = spl
            if key != b'version':
                store[key] = value

class Server(socketserver.ThreadingUDPServer):
    allow_reuse_address = True

server = Server(('0.0.0.0', 9999), Handler)
server.serve_forever()
