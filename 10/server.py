from collections import namedtuple, defaultdict
import socket
import socketserver

File = namedtuple('File', 'data rev prev')
Dir = namedtuple('Dir', 'contents')
Node = namedtuple('Node', 'filepart dirpart')

def mknode():
    return Node(None, Dir(defaultdict(mknode)))

root = mknode()

def get_file(filename):
    node = root
    path = filename.split(b'/')[1:]
    # If asking for a directory, remove the empty string
    if path[-1] == b'':
        path.pop()
    parent = None
    name = None
    for name in path:
        parent = node.dirpart
        node = node.dirpart.contents[name]
    return node, parent, name

def validate_filename(filename, allowdir=False):
    if not filename.startswith(b'/'):
        return False
    # validate each character is in range
    for c in filename:
        if c >= ord('A') and c <= ord('Z') \
            or c >= ord('a') and c <= ord('z') \
            or c >= ord('0') and c <= ord('9') \
            or c in b'.-_/':
                continue
        return False
    # some full-filename validation
    if allowdir and filename == b'/':
        return True
    return not filename.endswith(b'/') and b'//' not in filename

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            self.do_handle()
        except socket.error:
            print("Closing connection after error")
            try:
                self.request.close()
            except socket.error:
                pass

    def do_handle(self):
    
        self.request.sendall(b'READY\n')
        trail = b''
        while True:
            buf = trail
            while buf.find(b'\n') == -1:
                data = self.request.recv(1024)
                if not data:
                    return
                buf += data
            #print("<<<", buf)
            idx = buf.index(b'\n')
            line, trail = buf[:idx], buf[idx + 1:]
            print(line)
            cmd, *args = line.split(b' ')
            cmd = cmd.upper()
            if cmd == b'PUT':
                if len(args) != 2:
                    self.request.sendall(b'ERR usage: PUT file length newline data\n')
                    continue
                filename, length = args
                if not validate_filename(filename):
                    self.request.sendall(b'ERR illegal file name\n')
                    continue
                node, parent, name = get_file(filename)
                try:
                    length = int(length)
                    if length < 0:
                        raise ValueError('negative length')
                except ValueError:
                    self.request.sendall(b'ERR invalid length\n')
                    continue
                data, trail = trail[:length], trail[length:]
                while len(data) < length:
                    chunk = self.request.recv(length - len(data))
                    if not chunk:
                        return
                    data += chunk
                    trail = b''
                non_text = False
                for c in data:
                    if c < 32 and c != 10 and c != 9 and c != 11 and c != 13 or c > 127:
                        non_text = True
                        self.request.sendall(b'ERR non-text content\n')
                        break
                if non_text:
                    continue
                if node.filepart is not None:
                    if data != node.filepart.data:
                        node = parent.contents[name] = node._replace(filepart=File(data, node.filepart.rev + 1, node.filepart))
                else:
                    node = parent.contents[name] = node._replace(filepart=File(data, 1, None))
                self.request.send(b'OK r' + str(node.filepart.rev).encode('ascii') + b'\n')
                self.request.sendall(b'READY\n')
            elif cmd == b'GET':
                if len(args) == 0 or len(args) > 2:
                    self.request.sendall(b'ERR usage: GET file [revision]\n')
                    continue
                filename = args[0]
                if not validate_filename(filename):
                    self.request.sendall(b'ERR illegal file name\n')
                    continue
                if len(args) > 1:
                    if args[1].startswith(b'r'):
                        try:
                            want_rev = int(args[1][1:])
                        except ValueError:
                            want_rev = -1
                    else:
                        want_rev = -1
                else:
                    want_rev = None # latest
                node, parent, name = get_file(filename)
                if node.filepart is None:
                    self.request.sendall(b'ERR no such file\n')
                else:
                    file = node.filepart
                    if want_rev is not None:
                        if want_rev < 1 or want_rev > file.rev:
                            self.request.sendall(b'ERR no such revision\n')
                            continue
                        while file is not None and file.rev != want_rev:
                            file = file.prev
                    if file is None:
                        self.request.sendall(b'ERR no such revision\n')
                        continue
                    data = file.data
                    self.request.send(b'OK ' + str(len(data)).encode('ascii') + b'\n')
                    self.request.send(data)
                    self.request.sendall(b'READY\n')
            elif cmd == b'LIST':
                if len(args) != 1:
                    self.request.sendall(b'ERR usage: LIST dir\n')
                    continue
                dirname = args[0]
                if not validate_filename(dirname, allowdir=True):
                    self.request.sendall(b'ERR illegal file name\n')
                    continue
                node, parent, name = get_file(dirname)
                entries = list(sorted(node.dirpart.contents.items(), key=lambda i: i[0]))
                self.request.send(b'OK ' + str(len(entries)).encode('ascii') + b'\n')
                for name, node in entries:
                    if node.filepart is not None:
                        self.request.send(name + b' r' + str(node.filepart.rev).encode('ascii') + b'\n')
                    else:
                        self.request.send(name + b'/ DIR\n')
                self.request.sendall(b'READY\n')
            elif cmd == b'HELP':
                self.request.sendall(b'OK usage: HELP|GET|PUT|LIST\nREADY\n')
            else:
                self.request.sendall(b'ERR illegal method: ' + cmd + b'\n')
                self.request.close()
                return

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

Server(('0.0.0.0', 9999), Handler).serve_forever()
