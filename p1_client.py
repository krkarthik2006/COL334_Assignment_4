import socket
import sys
import struct
import time
import select

MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
REQUEST_TIMEOUT = 2.0
MAX_REQUEST_RETRIES = 5
EOF_MARKER = b"EOF"
ACK_INTERVAL = 0.005
DELAYED_ACK_THRESHOLD = 1

class ReliableUDPClient:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)
        except:
            pass

        self.socket.settimeout(REQUEST_TIMEOUT)

        self.received_data = {}
        self.next_expected = 0
        self.last_ack_sent_time = 0
        self.packets_since_ack = 0

    def parse_packet(self, packet):
        if len(packet) < HEADER_SIZE:
            return None, None

        seq_num = struct.unpack('!I', packet[:4])[0]
        data = packet[HEADER_SIZE:]
        return seq_num, data

    def create_ack(self, ack_num, sack_blocks=None):
        ack_packet = struct.pack('!I', ack_num)

        if sack_blocks and len(sack_blocks) > 0:
            sack_data = b''
            for start, end in sack_blocks[:2]:
                sack_data += struct.pack('!II', start, end)
            sack_data += b'\x00' * (16 - len(sack_data))
            ack_packet += sack_data
        else:
            ack_packet += b'\x00' * 16

        return ack_packet

    def get_sack_blocks(self):
        if not self.received_data:
            return []

        sack_blocks = []
        sorted_seqs = sorted(self.received_data.keys())

        i = 0
        while i < len(sorted_seqs):
            seq = sorted_seqs[i]
            if seq <= self.next_expected:
                i += 1
                continue

            block_start = seq
            block_end = seq + len(self.received_data[seq])

            j = i + 1
            while j < len(sorted_seqs):
                next_seq = sorted_seqs[j]
                if next_seq <= block_end:
                    block_end = max(block_end, next_seq + len(self.received_data[next_seq]))
                    j += 1
                else:
                    break

            sack_blocks.append((block_start, block_end))
            i = j

        return sack_blocks

    def send_ack(self, force=False):
        sack_blocks = self.get_sack_blocks()
        ack_packet = self.create_ack(self.next_expected, sack_blocks)
        self.socket.sendto(ack_packet, (self.server_ip, self.server_port))
        self.last_ack_sent_time = time.time()
        self.packets_since_ack = 0

    def request_file(self):
        request_msg = b'R'

        for attempt in range(MAX_REQUEST_RETRIES):
            try:
                self.socket.sendto(request_msg, (self.server_ip, self.server_port))
                print(f"Sent file request (attempt {attempt + 1}/{MAX_REQUEST_RETRIES})")

                ready = select.select([self.socket], [], [], REQUEST_TIMEOUT)
                if ready[0]:
                    return True

            except socket.timeout:
                print(f"Request timeout (attempt {attempt + 1}/{MAX_REQUEST_RETRIES})")

        print("Failed to connect to server after maximum retries")
        return False

    def receive_file(self):
        if not self.request_file():
            return False

        self.socket.setblocking(False)

        packets_received = 0
        total_bytes = 0
        eof_received = False
        eof_seq_num = None
        last_activity = time.time()
        stall_timeout = 3.0

        print("Receiving file...")

        while True:
            if time.time() - last_activity > stall_timeout:
                print("Connection stalled, finishing...")
                break

            ready = select.select([self.socket], [], [], 0.01)

            if ready[0]:
                try:
                    packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                    last_activity = time.time()

                    seq_num, data = self.parse_packet(packet)

                    if seq_num is not None and data is not None:
                        if data == EOF_MARKER:
                            print(f"Received EOF marker at sequence {seq_num}")
                            eof_received = True
                            eof_seq_num = seq_num

                            if seq_num not in self.received_data:
                                self.received_data[seq_num] = data

                            while self.next_expected in self.received_data:
                                self.next_expected += len(self.received_data[self.next_expected])

                            self.send_ack()

                            if self.next_expected >= eof_seq_num:
                                print("All data before EOF received, finishing...")
                                break
                            else:
                                print(f"Still waiting for data: next_expected={self.next_expected}, EOF at {eof_seq_num}")
                                continue

                        if seq_num not in self.received_data:
                            self.received_data[seq_num] = data
                            packets_received += 1
                            total_bytes += len(data)
                            self.packets_since_ack += 1

                            while self.next_expected in self.received_data:
                                self.next_expected += len(self.received_data[self.next_expected])

                            if eof_received and eof_seq_num is not None and self.next_expected >= eof_seq_num:
                                print("All data received after EOF, finishing...")
                                self.send_ack()
                                break

                            self.send_ack()
                        else:
                            self.send_ack()

                except socket.error:
                    pass

            current_time = time.time()
            if current_time - self.last_ack_sent_time >= 0.01:
                self.send_ack()

        self.send_ack()
        time.sleep(0.01)
        self.send_ack()

        print(f"Received {packets_received} packets, {total_bytes} bytes")

        return self.write_file()

    def write_file(self):
        try:
            with open('received_data.txt', 'wb') as f:
                current_seq = 0
                sorted_seqs = sorted(self.received_data.keys())

                for seq in sorted_seqs:
                    data = self.received_data[seq]
                    if data == EOF_MARKER:
                        continue

                    if seq == current_seq:
                        f.write(data)
                        current_seq += len(data)
                    elif seq > current_seq:
                        print(f"Warning: Gap detected at sequence {current_seq}, next is {seq}")
                        current_seq = seq
                        f.write(data)
                        current_seq += len(data)

            print(f"File written to received_data.txt ({current_seq} bytes)")
            return True

        except Exception as e:
            print(f"Error writing file: {e}")
            return False

    def run(self):
        """Main client execution"""
        print(f"Connecting to server at {self.server_ip}:{self.server_port}")

        try:
            success = self.receive_file()
            if success:
                print("File transfer completed successfully")
            else:
                print("File transfer failed")

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.socket.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 p1_client.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])

    client = ReliableUDPClient(server_ip, server_port)
    client.run()

if __name__ == "__main__":
    main()
