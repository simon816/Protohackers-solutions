from collections import namedtuple, defaultdict
import functools
import socket
import struct
import socketserver
import threading
import time

MHello = namedtuple('MHello', 'protocol version')
MError = namedtuple('MError', 'message')
MOK = namedtuple('MOK', '')
MDialAuthority = namedtuple('MDialAuthority', 'site')
MTargetPopulations = namedtuple('MTargetPopulations', 'site populations')
MCreatePolicy = namedtuple('MCretePolicy', 'species action')
MDeletePolicy = namedtuple('MDeletePolicy', 'policy')
MPolicyResult = namedtuple('MPolicyResult', 'policy')
MSiteVisit = namedtuple('MSiteVisit', 'site populations')

message_id = {}
message_id[MHello] = 0x50
message_id[MError] = 0x51
message_id[MOK] = 0x52
message_id[MDialAuthority] = 0x53
message_id[MTargetPopulations] = 0x54
message_id[MCreatePolicy] = 0x55
message_id[MDeletePolicy] = 0x56
message_id[MPolicyResult] = 0x57
message_id[MSiteVisit] = 0x58

def get_u32(idx, data):
    assert idx <= len(data) - 4, "bad u32"
    val, = struct.unpack_from('!I', data, idx)
    return val, idx + 4

def get_string(idx, data):
    s_len, idx = get_u32(idx, data)
    assert s_len <= len(data) - idx, "bad string length"
    return data[idx:idx + s_len].decode('ascii'), idx + s_len

def get_array(idx, data, spec):
    a_len, idx = get_u32(idx, data)
    arr = []
    for _ in range(a_len):
        elem = {}
        for (name, type) in spec:
            if type == 'u32':
                val, idx = get_u32(idx, data)
            elif type == 'str':
                val, idx = get_string(idx, data)
            else:
                assert False, "Unknown data type"
            elem[name] = val
        arr.append(elem)
    return arr, idx

def read_message(sock):
    start_bytes = bytearray()
    while len(start_bytes) != 5:
        buf = sock.recv(5 - len(start_bytes))
        if not buf:
            return None
        start_bytes.extend(buf)
    if not start_bytes:
        return None
    m_type, total_len = struct.unpack('!BI', start_bytes)
    print("Size:", total_len)
    assert total_len < 128 * 1024 * 1024, "message too big"
    data_len = total_len - len(start_bytes)
    data = bytearray()
    while len(data) != data_len:
        buf = sock.recv(data_len - len(data))
        if not buf:
            return None
        data.extend(buf)
    assert (sum(start_bytes) + sum(data)) % 256 == 0, "bad checksum"
    idx = 0
    m = None
    if m_type == 0x50:
        protocol, idx = get_string(idx, data)
        version, idx = get_u32(idx, data)
        assert protocol == "pestcontrol", "bad protocol"
        assert version == 1, "bad version %d" % version
        m = MHello(protocol, version)
    elif m_type == 0x51:
        message, idx = get_string(idx, data)
        m = MError(message)
    elif m_type == 0x52:
        m = MOK()
    elif m_type == 0x53:
        site, idx = get_u32(idx, data)
        m = MDialAuthority(site)
    elif m_type == 0x54:
        site, idx = get_u32(idx, data)
        populations, idx = get_array(idx, data, (('species', 'str'), ('min', 'u32'), ('max', 'u32')))
        m = MTargetPopulations(site, populations)
    elif m_type == 0x55:
        species, idx = get_string(idx, data)
        action = data[idx]
        idx += 1
        m = MCreatePolicy(species, action)
    elif m_type == 0x56:
        policy, idx = get_u32(idx, data)
        m = MDeletePolicy(policy)
    elif m_type == 0x57:
        policy, idx = get_u32(idx, data)
        m = MPolicyResult(policy)    
    elif m_type == 0x58:
        site, idx = get_u32(idx, data)
        populations, idx = get_array(idx, data, (('species', 'str'), ('count', 'u32')))
        m = MSiteVisit(site, populations)
    else:
        assert False, "Unknown message: %s" % m_type
    assert idx == len(data) - 1, "Unconsumed input: %s/%s" % (idx, len(data)) # index up to data length minus checksum length
    return m

def add_u32(val, buf):
    buf.extend(struct.pack('!I', val))

def add_str(val, buf):
    add_u32(len(val), buf)
    buf.extend(val.encode('ascii'))

def add_byte(val, buf):
    buf.append(val & 0xFF)

def encode_value(val, enc):
    if type(val) == int:
        add_u32(val, enc)
    elif type(val) == str:
        add_str(val, enc)
    elif type(val) == list:
        add_u32(len(val), enc)
        for el in val:
            encode_value(el)
    return enc

def write_message(sock, message):
    msg = bytearray()
    for field in message._fields:
        val = getattr(message, field)
        if type(message) == MCreatePolicy and field == 'action':
            add_byte(val, msg)
        else:
            encode_value(val, msg)
    msg_len = len(msg) + 6
    full = bytearray([message_id[type(message)]])
    add_u32(len(msg) + 6, full)
    full.extend(msg)
    checksum = (256 - (sum(full) % 256)) % 256
    full.append(checksum)
    sock.sendall(full)

@functools.lru_cache(maxsize=None)
def get_auth_for_site(site):
    lock = threading.RLock()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('pestcontrol.protohackers.com', 20547))
    try:
        assert type(read_message(sock)) == MHello, "First message must be hello"
    except AssertionError as e:
        write_message(MError(str(e.args[0])))
        raise
    write_message(sock, MHello("pestcontrol", 1))
    write_message(sock, MDialAuthority(site))
    resp = read_message(sock)
    print("Got", resp)
    assert type(resp) == MTargetPopulations, "expecting TargetPopulations"
    assert resp.site == site, "Different site"
    targets = {}
    for elem in resp.populations:
        targets[elem['species']] = (elem['min'], elem['max'])
    return sock, lock, targets

site_policies = defaultdict(dict)

def delete_policy(site, species):
    auth_sock, auth_lock, _ = get_auth_for_site(site)
    if species in site_policies[site]:
        auth_lock.acquire()
        print("Delete policy", (site, species))
        write_message(auth_sock, MDeletePolicy(site_policies[site][species]))
        resp = read_message(auth_sock)
        auth_lock.release()
        assert type(resp) == MOK, "Not ok"

def set_policy(site, species, action):
    auth_sock, auth_lock, _ = get_auth_for_site(site)
    auth_lock.acquire()
    delete_policy(site, species)
    print("Set policy", (site, species, action))
    write_message(auth_sock, MCreatePolicy(species, action))
    resp = read_message(auth_sock)
    auth_lock.release()
    assert type(resp) == MPolicyResult, "expecting PolicyResult"
    site_policies[site][species] = resp.policy

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        while True:
            try:
                self.try_handle()
            except AssertionError as e:
                print("error:", e)
                write_message(self.request, MError(str(e.args[0])))
                time.sleep(2)
                break
            break
        self.request.close()


    def try_handle(self):
        write_message(self.request, MHello("pestcontrol", 1))
        hello = read_message(self.request)
        assert type(hello) == MHello, 'First message must be Hello'

        while True:
            m = read_message(self.request)
            print(m)
            if m is None or type(m) == MError:
                return
            if type(m) == MSiteVisit:
                counts = {}
                for elem in m.populations:
                    if elem['species'] in counts:
                        assert elem['count'] == counts[elem['species']], "duplicate conflict"
                    counts[elem['species']] = elem['count']
                _, _, target = get_auth_for_site(m.site)
                for species, (c_min, c_max) in target.items():
                    count = counts.get(species, 0)
                    if count < c_min:
                        set_policy(m.site, species, 0xa0)
                    elif count > c_max:
                        set_policy(m.site, species, 0x90)
                    else:
                        delete_policy(m.site, species)
                write_message(self.request, MOK())
            else:
                assert False, "unexpected message type"

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

Server(('0.0.0.0', 9999), Handler).serve_forever()
