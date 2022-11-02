import bisect
from collections import namedtuple
import struct
import socketserver
import time
import threading

class ProtocolError(Exception):

    def __init__(self, msg):
        self.msg = msg

beat_counter = {}

class BeatCounter:

    def __init__(self, client, interval):
        self.client = client
        self.interval = interval
        self.acc = 0

    def beat(self):
        self.acc += 1
        if self.acc == self.interval:
            self.acc = 0
            self.send_beat()

    def send_beat(self):
        self.client.send(0x41, b'')

def register_heartbeat(client, interval):
    beat_counter[id(client)] = BeatCounter(client, interval)

def unregister_heartbeat(client):
    if id(client) in beat_counter:
        del beat_counter[id(client)]

def heartbeat_thread():
    while True:
        time.sleep(.1)
        for counter in beat_counter.values():
            counter.beat()

class Road:

    def __init__(self, road_id):
        self.id = road_id
        self.limit = None
        self.camera_to_pos = {}
        self.position_to_camera = {}
        self.dispatchers = {}
        self.stored_tickets = []
        # observations per car
        self.car_observations = {}

    def set_limit(self, limit):
        if self.limit is not None and self.limit != limit:
            raise ProtocolError('Limit different to previous limit')
        self.limit = limit

    def add_camera(self, camera, position):
        if position in self.position_to_camera:
            raise ProtocolError('Camera already exists at this location')
        self.position_to_camera[position] = camera
        self.camera_to_pos[id(camera)] = position

    def add_dispatcher(self, dispatcher):
        self.dispatchers[id(dispatcher)] = dispatcher
        # If we were storing tickets, flush them now
        tickets = list(self.stored_tickets)
        self.stored_tickets = []
        for ticket in tickets:
            self.send_ticket(ticket)

    def remove_camera(self, camera):
        pos = self.camera_to_pos[id(camera)]
        del self.camera_to_pos[id(camera)]
        del self.position_to_camera[pos]

    def remove_dispatcher(self, dispatcher):
        del self.dispatchers[id(dispatcher)]

    def camera_observation(self, camera, plate, timestamp):
        pos = self.camera_to_pos[id(camera)]
        if plate not in self.car_observations:
            # store timestamp and pos in two synchronised lists
            self.car_observations[plate] = ([], [])
        obs_ts, obs_pos = self.car_observations[plate]
        idx = bisect.bisect(obs_ts, timestamp)
        obs_ts.insert(idx, timestamp)
        obs_pos.insert(idx, pos)
        print("Observations:", plate, list(zip(obs_ts, obs_pos)))
        for speed, obs1, obs2 in get_speeds(idx, obs_ts, obs_pos):
            print("Speed", speed, obs1, obs2)
            if round(speed) > self.limit:
                self.create_ticket(plate, speed, obs1, obs2)

    def create_ticket(self, plate, speed, obs1, obs2):
        speed_int = int(round(speed * 100))
        ticket = (plate, speed_int, obs1, obs2)
        # send now, or store for later
        if self.dispatchers:
            self.send_ticket(ticket)
        else:
            print("Store ticket", ticket)
            self.stored_tickets.append(ticket)

    def send_ticket(self, ticket):
        print("Maybe send ticket", ticket)
        plate, speed, obs1, obs2 = ticket
        pos1, time1 = obs1
        pos2, time2 = obs2
        if should_send_ticket(plate, time1, time2):
            print("Will send ticket", ticket)
            # choose arbitrarily
            dispatcher = next(iter(self.dispatchers.values()))
            plate_bytes = plate.encode('ascii')
            msg = struct.pack('!B', len(plate_bytes))
            msg += plate_bytes
            msg += struct.pack('!HHIHIH', self.id, pos1, time1, pos2, time2, speed)
            dispatcher.send(0x21, msg)

# Returns up to two speed observations
# tuple of: (speed, first_obs, second_obs)
def get_speeds(idx, times, positions):
    if len(times) == 1:
        return []
    pos = positions[idx]
    time = times[idx]
    # If there was an obervation earlier
    if idx > 0:
        prev_pos = positions[idx - 1]
        prev_time = times[idx - 1]
        dist = abs(pos - prev_pos)
        t = time - prev_time
        yield ((dist / t) * 3600, (prev_pos, prev_time), (pos, time))
    # If there is an observation later
    if idx < len(times) - 1:
        new_pos = positions[idx + 1]
        new_time = times[idx + 1]
        dist = abs(new_pos - pos)
        t = new_time - time
        yield ((dist / t) * 3600, (pos, time), (new_pos, new_time))

# maps cameras to a Road object
camera_to_road = {}
# maps dispatchers to a list of roads
dispatcher_roads = {}
# maps roads to Road objects
roads = {}
# maps cars to days ticketed
car_tickets = {}

def should_send_ticket(plate, time1, time2):
    if plate not in car_tickets:
        car_tickets[plate] = set()
    day_start = time1 // 86400
    day_end = time2 // 86400
    days = set()
    for day in range(day_start, day_end + 1):
        days.add(day)
        if day in car_tickets[plate]:
            return False
    car_tickets[plate].update(days)
    return True

def register_camera(client, road, mile, limit):
    print("Register camera", { 'client': id(client), 'road': road, 'mile': mile, 'limit': limit })
    if road not in roads:
        roads[road] = Road(road)
    road_obj = roads[road]
    road_obj.set_limit(limit)
    road_obj.add_camera(client, mile)
    camera_to_road[id(client)] = road_obj

def unregister_camera(client):
    camera_to_road[id(client)].remove_camera(client)
    del camera_to_road[id(client)]

def register_dispatcher(client, in_roads):
    print("Register dispatcher", { 'client': id(client), 'roads': in_roads })
    road_objs = []
    for road in in_roads:
        if road not in roads:
            roads[road] = Road(road)
        road_objs.append(roads[road])
        roads[road].add_dispatcher(client)
    dispatcher_roads[id(client)] = road_objs

def unregister_dispatcher(client):
    road_objs = dispatcher_roads[id(client)]
    for road in road_objs:
        road.remove_dispatcher(client)
    del dispatcher_roads[id(client)]

def camera_observation(camera, plate, timestamp):
    print("Observation", { 'camera': id(camera), 'plate': plate, 'timestamp': timestamp })
    road = camera_to_road[id(camera)]
    road.camera_observation(camera, plate, timestamp)

class Handler(socketserver.BaseRequestHandler):

    def handle(self):

        self.client_type = None
        self.heartbeat_known = False

        while True:
            try:
                self.main_loop()
            except ProtocolError as e:
                err = e.msg.encode('ascii')
                try:
                    self.send(0x10, struct.pack('!B', len(err)) + err)
                    # Why was this needed??
                    time.sleep(1)
                except Exception:
                    pass
                break
            except Exception as e:
                print("Exception:", e)
                # we still need to handle teardown
                break

        self.request.close()
        if self.client_type == 'camera':
            unregister_camera(self)
        elif self.client_type == 'dispatcher':
            unregister_dispatcher(self)
        unregister_heartbeat(self)

    def main_loop(self):
        msg_type = self.read_u8()
        if msg_type == 0x20:
            # Plate
            if self.client_type != 'camera':
                raise ProtocolError('not a camera')
            plate = self.read_str()
            timestamp = self.read_u32()
            camera_observation(self, plate, timestamp)
        elif msg_type == 0x40:
            # WantHeartbeat
            if self.heartbeat_known:
                raise ProtocolError('Heartbeat already set')
            interval = self.read_u32()
            if interval != 0:
                register_heartbeat(self, interval)
            self.heartbeat_known = True
        elif msg_type == 0x80:
            # IAmCamera
            if self.client_type is not None:
                raise ProtocolError('Already classified as another type')
            road = self.read_u16()
            mile = self.read_u16()
            limit = self.read_u16()
            register_camera(self, road, mile, limit)
            self.client_type = 'camera'
        elif msg_type == 0x81:
            if self.client_type is not None:
                raise ProtocolError('Already classified as another type')
            # IAmDispatcher
            numroads = self.read_u8()
            roads = []
            for _  in range(numroads):
                roads.append(self.read_u16())
            register_dispatcher(self, roads)
            self.client_type = 'dispatcher'
        else:
            raise ProtocolError('Unknown message type')

    def read_u8(self):
        return self._read(1)[0]

    def read_u16(self):
        (val,) = struct.unpack('!H', self._read(2))
        return val

    def read_u32(self):
        (val,) = struct.unpack('!I', self._read(4))
        return val

    def read_str(self):
        l = self.read_u8()
        return self._read(l).decode('ascii')

    def _read(self, size):
        buf = b''
        while len(buf) < size:
            remaining = size - len(buf)
            recv = self.request.recv(remaining)
            # not a protocol error, so don't attempt to send an error
            if not len(recv):
                raise Exception('Unable to read')
            buf += recv
        return buf

    def send(self, msg_id, data):
        self.request.send(struct.pack('!B', msg_id))
        self.request.sendall(data)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

t = threading.Thread(target=heartbeat_thread)
t.daemon = True
t.start()

server = Server(('0.0.0.0', 9999), Handler)
server.serve_forever()
