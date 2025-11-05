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
WRITE_BUFFER_SIZE = 512 * 1024

class ReliableUDPClient:
    def __init__(self, server_ip, server_port, pref_filename):
        self.server_ip = server_ip
        self.server_port = server_port
        self.pref_filename = pref_filename
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16777216)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216)
        except:
            pass

        self.socket.settimeout(REQUEST_TIMEOUT)

        self.received_data = {}
        self.next_expected = 0
        self.last_ack_sent_time = 0
        self.packets_since_ack = 0
        self.write_buffer = bytearray()

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
            if seq < self.next_expected:
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

    def flush_write_buffer(self, file_handle):
        if len(self.write_buffer) > 0:
            file_handle.write(bytes(self.write_buffer))
            self.write_buffer.clear()

    def buffered_write(self, file_handle, data):
        self.write_buffer.extend(data)

        if len(self.write_buffer) >= WRITE_BUFFER_SIZE:
            self.flush_write_buffer(file_handle)

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

        print("Failed to connect to server")
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

        output_filename = f"{self.pref_filename}received_data.txt"

        try:
            with open(output_filename, 'wb') as file_handle:
                print("Receiving file...")

                while True:
                    current_time = time.time()

                    if current_time - last_activity > stall_timeout:
                        if eof_received and self.next_expected == eof_seq_num:
                            print("Stall timeout after EOF, transfer complete")
                        else:
                            print(f"Connection stalled. next_expected={self.next_expected}, eof_seq={eof_seq_num}")
                        break

                    if current_time - self.last_ack_sent_time >= 0.02:
                        self.send_ack()

                    ready = select.select([self.socket], [], [], 0.001)

                    if not ready[0]:
                        continue

                    packets_this_round = 0
                    for _ in range(100):
                        try:
                            packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                            last_activity = time.time()
                            packets_this_round += 1
                            
                            seq_num, data = self.parse_packet(packet)

                            if seq_num is None or data is None:
                                continue

                            if data == EOF_MARKER:
                                print(f"Received EOF at sequence {seq_num}")
                                eof_received = True
                                eof_seq_num = seq_num

                                if seq_num == self.next_expected:
                                    self.flush_write_buffer(file_handle)
                                    print("EOF received in order, finishing")
                                    self.send_ack()
                                    time.sleep(0.01)
                                    self.send_ack()
                                    return True
                                else:
                                    self.send_ack()
                                    continue

                            if seq_num > self.next_expected:
                                if seq_num not in self.received_data:
                                    self.received_data[seq_num] = data
                                    packets_received += 1
                                    total_bytes += len(data)
                                self.send_ack()
                                continue

                            if seq_num == self.next_expected:
                                self.buffered_write(file_handle, data)
                                self.next_expected += len(data)
                                packets_received += 1
                                total_bytes += len(data)

                                while self.next_expected in self.received_data:
                                    buffered_data = self.received_data.pop(self.next_expected)
                                    self.buffered_write(file_handle, buffered_data)
                                    self.next_expected += len(buffered_data)

                                self.send_ack()

                                if eof_received and self.next_expected == eof_seq_num:
                                    self.flush_write_buffer(file_handle)
                                    print("All data received after EOF, finishing")
                                    self.send_ack()
                                    time.sleep(0.01)
                                    self.send_ack()
                                    return True

                            elif seq_num < self.next_expected:
                                self.send_ack()

                        except socket.error:
                            break

                    if len(self.write_buffer) > WRITE_BUFFER_SIZE // 2:
                        self.flush_write_buffer(file_handle)

                self.flush_write_buffer(file_handle)

        except Exception as e:
            print(f"Error during file receive: {e}")
            import traceback
            traceback.print_exc()
            return False

        print(f"Received {packets_received} packets, {total_bytes} bytes")
        print(f"File written to {output_filename} ({self.next_expected} bytes)")

        # Send final ACKs
        for _ in range(3):
            self.send_ack()
            time.sleep(0.01)

        if eof_received and self.next_expected == eof_seq_num:
            return True
        else:
            print(f"Transfer incomplete. EOF: {eof_received}, Expected: {self.next_expected}, EOF seq: {eof_seq_num}")
            return False

    def run(self):
        print(f"Client connecting to {self.server_ip}:{self.server_port}")

        success = False
        try:
            success = self.receive_file()
            if success:
                print("File transfer completed successfully")
            else:
                print("File transfer failed or incomplete")

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.socket.close()
            print("Client socket closed")

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 p2_client_improved.py <SERVER_IP> <SERVER_PORT> <PREF_FILENAME>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    pref_filename = sys.argv[3]

    client = ReliableUDPClient(server_ip, server_port, pref_filename)
    client.run()

if __name__ == "__main__":
    main()