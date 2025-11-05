import socket
import sys
import struct
import time
import select

MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
MAX_DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
EOF_MARKER = b"EOF"
INITIAL_TIMEOUT = 0.05
TIMEOUT_MULTIPLIER = 1.2
MAX_TIMEOUT = 0.4
MIN_TIMEOUT = 0.02
SEND_BATCH_SIZE = 20

class ReliableUDPServer:
    def __init__(self, server_ip, server_port, sws):
        self.server_ip = server_ip
        self.server_port = server_port
        self.sws = sws
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)
        except:
            pass

        self.socket.bind((server_ip, server_port))
        self.socket.settimeout(30.0)

        self.srtt = None
        self.rttvar = None
        self.rto = INITIAL_TIMEOUT

    def calculate_rto(self, sample_rtt):
        alpha = 0.125
        beta = 0.25

        if self.srtt is None:
            self.srtt = sample_rtt
            self.rttvar = sample_rtt / 2
        else:
            prev_rttvar = float(self.rttvar) if self.rttvar is not None else (sample_rtt / 2)
            self.rttvar = (1 - beta) * prev_rttvar + beta * abs(self.srtt - sample_rtt)
            self.srtt = (1 - alpha) * self.srtt + alpha * sample_rtt

        self.rto = self.srtt + 4 * self.rttvar
        if self.rto < MIN_TIMEOUT:
            self.rto = MIN_TIMEOUT
        elif self.rto > MAX_TIMEOUT:
            self.rto = MAX_TIMEOUT

    def create_packet(self, seq_num, data):
        header = struct.pack('!I', seq_num) + b'\x00' * 16
        return header + data

    def parse_ack(self, packet):
        if len(packet) < 4:
            return None, None

        ack_num = struct.unpack('!I', packet[:4])[0]

        sack_blocks = []
        if len(packet) >= 20:
            reserved = packet[4:20]
            for i in range(0, 16, 8):
                if i + 8 <= len(reserved):
                    start, end = struct.unpack('!II', reserved[i:i+8])
                    if start > 0 and end > start:
                        sack_blocks.append((start, end))

        return ack_num, sack_blocks

    def send_file(self, client_addr):
        try:
            import os
            total_size = os.path.getsize('data.txt')
            file_handle = open('data.txt', 'rb')
        except FileNotFoundError:
            print("Error: data.txt not found")
            return

        print(f"Sending file of size {total_size} bytes to {client_addr}")

        base = 0
        next_seq = 0
        window_packets = {}
        dup_ack_count = {}
        last_ack_received = 0

        self.socket.setblocking(False)

        total_packets_sent = 0
        retransmissions = 0

        while base <= total_size:
            packets_sent_in_batch = 0
            while next_seq < total_size and (next_seq - base) < self.sws:
                chunk_start = next_seq
                chunk_end = min(next_seq + MAX_DATA_SIZE, total_size)

                file_handle.seek(chunk_start)
                data = file_handle.read(chunk_end - chunk_start)

                packet = self.create_packet(next_seq, data)
                self.socket.sendto(packet, client_addr)

                window_packets[next_seq] = (packet, time.time(), 0)
                next_seq = chunk_end
                total_packets_sent += 1
                packets_sent_in_batch += 1

                if packets_sent_in_batch >= SEND_BATCH_SIZE:
                    break

            if next_seq == total_size and total_size not in window_packets:
                eof_packet = self.create_packet(total_size, EOF_MARKER)
                self.socket.sendto(eof_packet, client_addr)
                window_packets[total_size] = (eof_packet, time.time(), 0)
                total_packets_sent += 1

            timeout = 0.01

            if base in window_packets:
                base_send_time = window_packets[base][1]
                elapsed = time.time() - base_send_time
                timeout = max(0.001, self.rto - elapsed)

            ready = select.select([self.socket], [], [], timeout)

            if ready[0]:
                try:
                    ack_packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                    recv_time = time.time()

                    ack_num, sack_blocks = self.parse_ack(ack_packet)

                    if ack_num is not None:
                        if ack_num > base and base in window_packets:
                            _, send_time, retrans_count = window_packets[base]
                            if retrans_count == 0:
                                sample_rtt = recv_time - send_time
                                self.calculate_rto(sample_rtt)

                        if ack_num > base:
                            for seq in list(window_packets.keys()):
                                if seq < ack_num:
                                    del window_packets[seq]
                            base = ack_num
                            last_ack_received = ack_num
                            dup_ack_count.clear()

                        elif ack_num == last_ack_received:
                            dup_ack_count[ack_num] = dup_ack_count.get(ack_num, 0) + 1

                            if dup_ack_count[ack_num] >= 3 and base in window_packets:
                                packet, _, retrans_count = window_packets[base]
                                self.socket.sendto(packet, client_addr)
                                window_packets[base] = (packet, time.time(), retrans_count + 1)
                                retransmissions += 1

                        if sack_blocks:
                            now = time.time()
                            for start, end in sack_blocks:
                                for seq in list(window_packets.keys()):
                                    if start <= seq < end and seq != base:
                                        del window_packets[seq]

                            if base in window_packets and sack_blocks:
                                max_sack_end = max(end for _, end in sack_blocks)
                                for seq in list(window_packets.keys()):
                                    if base < seq < max_sack_end:
                                        is_sacked = any(start <= seq < end for start, end in sack_blocks)
                                        if not is_sacked:
                                            packet, send_time, retrans_count = window_packets[seq]
                                            self.socket.sendto(packet, client_addr)
                                            window_packets[seq] = (packet, now, retrans_count + 1)
                                            retransmissions += 1

                except socket.error:
                    pass

            if window_packets:
                current_time = time.time()
                if base in window_packets:
                    packet, send_time, retrans_count = window_packets[base]
                    if current_time - send_time > self.rto:
                        self.socket.sendto(packet, client_addr)
                        window_packets[base] = (packet, current_time, retrans_count + 1)
                        retransmissions += 1

                        if retrans_count >= 2:
                            self.rto = min(MAX_TIMEOUT, self.rto * TIMEOUT_MULTIPLIER)

        file_handle.close()

        print(f"File transfer complete. Total packets: {total_packets_sent}, Retransmissions: {retransmissions}")

    def run(self):
        print(f"Server listening on {self.server_ip}:{self.server_port} with SWS={self.sws}")

        try:
            _, client_addr = self.socket.recvfrom(MAX_PACKET_SIZE)
            print(f"Received request from {client_addr}")

            self.send_file(client_addr)

        except socket.timeout:
            print("No client request received")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.socket.close()

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 p1_server.py <SERVER_IP> <SERVER_PORT> <SWS>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    sws = int(sys.argv[3])

    server = ReliableUDPServer(server_ip, server_port, sws)
    server.run()

if __name__ == "__main__":
    main()

