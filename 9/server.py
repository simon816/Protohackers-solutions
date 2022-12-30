import json
from queue import PriorityQueue, Empty
import socket
import selectors
from collections import namedtuple, defaultdict

Client = namedtuple('Client', 'sock line_buf working_on waits')
Wait = namedtuple('Wait', 'client_id queues')

clients = {}
valid_jobs = set()
assigned = {}

job_counter = 0

class Queue:

    def __init__(self):
        self.q = PriorityQueue()
        self.counter = 0
        self.waiters = set()

    def put(self, pri, data):
        global job_counter
        id = job_counter
        job_counter += 1
        self.q.put((-pri, id, data))
        valid_jobs.add(id)
        return id

    def insert(self, id, pri, data):
        self.q.put((pri, id, data))
        valid_jobs.add(id)

    def peek(self):
        while True:
            try:
                job = self.q.get_nowait()
            except Empty:
                return None
            id = job[1]
            if id in valid_jobs:
                break
        self.q.put(job)
        pri, id, data = job
        return id, pri, data

    def pop(self):
        pri, id, data = self.q.get_nowait()
        assert id in valid_jobs
        valid_jobs.remove(id)
        return id, pri, data

    def add_wait(self, wait):
        self.waiters.add(wait)

    def remove_wait(self, wait):
        self.waiters.discard(wait)

queues = defaultdict(Queue)

def register_client(sock):
    no = sock.fileno()
    clients[no] = Client(sock, bytearray(), {}, set())

def on_disconnect(client):
    for id, job in client.working_on.items():
        del assigned[id]
        queues[job['queue']].insert(id, job['pri'], job['data'])
    for wait in client.waits:
        for queue in wait.queues:
            queues[queue].remove_wait(wait)
    no = client.sock.fileno()
    client.sock.close()
    del clients[no]
    for job in client.working_on.values():
        if queues[job['queue']].waiters:
            wait = queues[job['queue']].waiters.pop()
            process_wait(wait, job['queue'])

def process_wait(wait, queue):
    client = clients[wait.client_id]
    client.waits.remove(wait)
    for queue in wait.queues:
        queues[queue].remove_wait(wait)
    pop_job_to_client(client, queue)

def pop_job_to_client(client, queue):
    job_id, pri, data = queues[queue].pop()
    client.working_on[job_id] = { 'id': job_id, 'pri': pri, 'data': data, 'queue': queue }
    assigned[job_id] = client
    send(client, id=job_id, job=data, pri=-pri, queue=queue)

def send(client, status='ok', **kwargs):
    data = json.dumps({ **kwargs, 'status': status })
    print(">>>", client.sock.fileno(), data)
    client.sock.sendall(data.encode('utf8') + b'\n')

def send_error(client, msg):
    send(client, status='error', error=msg)

def on_response(sock):
    no = sock.fileno()
    client = clients[no]
    while True:
        try:
            buf = client.sock.recv(4096)
        except BlockingIOError:
            break
        except IOError:
            on_disconnect(client)
            return True
        if not buf:
            on_disconnect(client)
            return True
        lines = buf.split(b'\n')
        if len(lines) == 1:
            client.line_buf.extend(buf)
        else:
            first = client.line_buf + lines[0]
            client.line_buf.clear()
            client.line_buf.extend(lines[-1])
            process_line(client, first)
            for line in lines[1:-1]:
                process_line(client, line)

def process_line(client, line):
    try:
        req = json.loads(line)
    except:
        send_error(client, "Invalid JSON")
        return
    print("<<<", client.sock.fileno(), req)
    if 'request' not in req:
        send_error(client, 'Missing \"request\" key')
        return
    req_type = req['request']
    if req_type == 'put':
        try:
            q = req['queue']
            pri = req['pri']
            data = req['job']
        except KeyError:
            send_error(client, 'Missing data')
            return
        if type(pri) is not int or pri < 0:
            send_error(client, 'bad priority')
            return
        if type(q) is not str:
            send_error(client, 'bad queue')
            return
        if type(data) is not dict:
            send_error(client, 'bad job')
            return
        job_id = queues[q].put(pri, data)
        if queues[q].waiters:
            wait = queues[q].waiters.pop()
            process_wait(wait, q)
        send(client, id=job_id)
    elif req_type == 'get':
        if 'queues' not in req or type(req['queues']) is not list:
            send_error(client, 'bad request')
            return
        highest = None
        highest_queue = None
        for queue in req['queues']:
            job = queues[queue].peek()
            if job is not None:
                id, pri, data = job
                if highest is None or pri < highest:
                    highest = pri
                    highest_queue = queue
        if highest is not None:
            pop_job_to_client(client, highest_queue)
        elif req.get('wait', False) == True:
            wait = Wait(client.sock.fileno(), tuple(req['queues']))
            client.waits.add(wait)
            for queue in req['queues']:
                queues[queue].add_wait(wait)
        else:
            send(client, status='no-job')
    elif req_type == 'delete':
        if 'id' not in req or type(req['id']) is not int:
            send_error(client, 'bad job ID')
            return
        id = req['id']
        if id in valid_jobs:
            valid_jobs.remove(id)
            send(client)
        elif id in assigned:
            assigned.pop(id).working_on.pop(id)
            send(client)
        else:
            send(client, status='no-job')
    elif req_type == 'abort':
        if 'id' not in req or type(req['id']) is not int:
            send_error(client, 'bad job ID')
            return
        id = req['id']
        if id not in client.working_on:
            send(client, status='no-job')
        else:
            assert assigned.pop(id) is client
            job = client.working_on.pop(id)
            queues[job['queue']].insert(id, job['pri'], job['data'])
            send(client)
    else:
        send_error(client, 'Invalid request type')
        return

if __name__ == '__main__':
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('0.0.0.0', 9999))
    server_sock.listen()

    selector = selectors.EpollSelector()
    selector.register(server_sock, selectors.EVENT_READ)
    try:
        while True:
            ready = selector.select(timeout=30)
            for key, _ in ready:
                sock = key.fileobj
                if sock is server_sock:
                    client, addr = sock.accept()
                    client.setblocking(False)
                    register_client(client)
                    selector.register(client, selectors.EVENT_READ)
                else:
                    disconnect = on_response(sock)
                    if disconnect:
                        selector.unregister(sock)
    finally:
        server_sock.close()
