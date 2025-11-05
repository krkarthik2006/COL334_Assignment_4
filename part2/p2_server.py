import socket
import sys
import struct
import time
import select
import csv
from collections import deque

MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
MAX_DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
EOF_MARKER = b"EOF"
INITIAL_TIMEOUT = 0.2
TIMEOUT_MULTIPLIER = 2.0
MAX_TIMEOUT = 3.0
MIN_TIMEOUT = 0.1
MAX_CWND = 2000 * MAX_DATA_SIZE

MSS = MAX_DATA_SIZE

class ReliableUDPServer:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.MSS = MSS

        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16777216)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216)
        except:
            pass

        self.socket.bind((server_ip, server_port))
        self.socket.settimeout(30.0)

        self.srtt = None
        self.rttvar = None
        self.rto = INITIAL_TIMEOUT

        self.cwnd = float(1* MSS)
        self.ssthresh = float(128 * MSS)

        self.W_max = 0
        self.K = 0
        self.epoch_start = None
        self.tcp_cwnd = float(self.cwnd)

        self.C = 0.3
        self.beta = 0.99

        self.last_ack = 0
        self.dup_ack_count = 0
        self.last_congestion_time = 0

        self.start_time = None
        self.log_data = []
        self.bytes_sent = 0
        self.bytes_acked = 0
        self.sending_rate_window = deque(maxlen=10)
        self.last_log_time = None

        self.file_buffer = None
        self.file_buffer_offset = 0

    def log_state(self, event="periodic", extra_info=None):
        if self.start_time is None:
            return

        current_time = time.time() - self.start_time

        if self.cwnd < self.ssthresh:
            mode = "slow_start"
        else:
            mode = "congestion_avoidance"

        sending_rate = 0
        if len(self.sending_rate_window) > 0:
            total_sent = sum(b for b, _ in self.sending_rate_window)
            time_span = self.sending_rate_window[-1][1] - self.sending_rate_window[0][1]
            if time_span > 0:
                sending_rate = total_sent / time_span

        throughput = self.bytes_acked / current_time if current_time > 0 else 0

        log_entry = {
            'time': current_time,
            'cwnd': self.cwnd,
            'ssthresh': self.ssthresh,
            'mode': mode,
            'event': event,
            'rto': self.rto,
            'W_max': self.W_max,
            'sending_rate': sending_rate,
            'throughput': throughput,
            'bytes_sent': self.bytes_sent,
            'bytes_acked': self.bytes_acked,
            'extra': extra_info or ''
        }
        self.log_data.append(log_entry)

    def save_logs(self, filename='cubic_logs.csv'):
        if not self.log_data:
            return

        fieldnames = ['time', 'cwnd', 'ssthresh', 'mode', 'event', 'rto', 'W_max',
                      'sending_rate', 'throughput', 'bytes_sent', 'bytes_acked', 'extra']

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.log_data)

        print(f"Logs saved to {filename}")

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

    def cubic_update(self, acked_bytes):
        if acked_bytes <= 0:
            return

        self.bytes_acked += acked_bytes

        if self.cwnd < self.ssthresh:
            old_cwnd = self.cwnd
            self.cwnd += acked_bytes
            self.tcp_cwnd = self.cwnd

            if self.cwnd > MAX_CWND:
                self.cwnd = MAX_CWND
                self.tcp_cwnd = self.cwnd

            if int(old_cwnd / MSS) < int(self.cwnd / MSS):
                self.log_state(event="slow_start_growth", extra_info=f"cwnd: {old_cwnd:.0f} -> {self.cwnd:.0f}")
            return
        if self.epoch_start is None:
            self.epoch_start = time.time()
            if self.W_max == 0:
                self.W_max = self.cwnd / MSS
            self.K = ((self.W_max * (1.0 - self.beta)) / self.C) ** (1.0 / 3.0)
            self.tcp_cwnd = self.cwnd
            self.log_state(event="epoch_start", extra_info=f"W_max={self.W_max:.2f}, K={self.K:.2f}s")

        t = time.time() - self.epoch_start

        target_mss = self.C * ((t - self.K) ** 3) + self.W_max
        target_cwnd = target_mss * MSS

        tcp_increment = acked_bytes * MSS / max(self.tcp_cwnd, MSS)
        self.tcp_cwnd += tcp_increment

        if target_cwnd > self.cwnd:
            cnt = self.cwnd / (target_cwnd - self.cwnd)
            if cnt < 1:
                cnt = 1
            increment = acked_bytes / cnt
        else:
            increment = acked_bytes * MSS / self.cwnd

        cwnd_cubic = self.cwnd + increment
        if self.tcp_cwnd > cwnd_cubic:
            self.cwnd = self.tcp_cwnd
        else:
            self.cwnd = cwnd_cubic

        if self.cwnd < MSS:
            self.cwnd = MSS
        elif self.cwnd > MAX_CWND:
            self.cwnd = MAX_CWND
            self.tcp_cwnd = self.cwnd

    def on_congestion_event(self, is_timeout=False):
        current_time = time.time()
        min_interval = 2*self.srtt if self.srtt else 0.2

        if current_time - self.last_congestion_time < min_interval:
            return

        self.last_congestion_time = current_time

        old_cwnd = self.cwnd
        self.W_max = self.cwnd / MSS

        new_cwnd = max(self.cwnd * self.beta, 2 * MSS)
        self.ssthresh = max(new_cwnd, 2 * MSS)

        if is_timeout:
            self.cwnd = max(new_cwnd * 0.5, 2 * MSS)
        else:
            self.cwnd = new_cwnd

        if self.W_max > 0:
            self.K = ((self.W_max * (1.0 - self.beta)) / self.C) ** (1.0 / 3.0)
        else:
            self.K = 0.0

        self.epoch_start = None
        self.tcp_cwnd = self.cwnd

        self.log_state(event="congestion", extra_info=f"cwnd: {old_cwnd:.0f} -> {self.cwnd:.0f}, ssthresh: {self.ssthresh:.0f}")

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

    def load_file_buffer(self, filename, chunk_size=10*1024*1024):
        try:
            with open(filename, 'rb') as f:
                self.file_buffer = f.read()
            return len(self.file_buffer)
        except FileNotFoundError:
            return None

    def get_data_chunk(self, start, end):
        if self.file_buffer is None:
            return None
        return self.file_buffer[start:end]

    def send_file(self, client_addr):
        total_size = self.load_file_buffer('data.txt')
        if total_size is None:
            print("Error: data.txt not found")
            return

        print(f"Sending file of size {total_size} bytes to {client_addr}")
        print(f"Initial cwnd: {self.cwnd/MSS:.1f} MSS, ssthresh: {self.ssthresh/MSS:.1f} MSS")

        base = 0
        next_seq = 0
        window_packets = {}

        self.socket.setblocking(False)

        total_packets_sent = 0
        retransmissions = 0

        self.last_ack = 0
        self.dup_ack_count = 0

        self.start_time = time.time()
        self.bytes_sent = 0
        self.bytes_acked = 0
        self.last_log_time = self.start_time
        self.log_state(event="transfer_start", extra_info=f"file_size={total_size}")

        max_burst_packets = 20
        pacer_interval = 0.00001

        while base <= total_size:
            in_flight = next_seq - base
            packets_in_this_loop = 0

            while next_seq < total_size and in_flight < self.cwnd and packets_in_this_loop < max_burst_packets:
                current_time = time.time()

                chunk_start = next_seq
                chunk_end = min(next_seq + MAX_DATA_SIZE, total_size)

                data = self.get_data_chunk(chunk_start, chunk_end)
                if data is None:
                    break

                packet = self.create_packet(next_seq, data)
                self.socket.sendto(packet, client_addr)

                bytes_in_packet = len(data)
                self.bytes_sent += bytes_in_packet
                self.sending_rate_window.append((bytes_in_packet, time.time()))

                window_packets[next_seq] = (packet, current_time, 0)
                next_seq = chunk_end
                total_packets_sent += 1

                in_flight = next_seq - base
                packets_in_this_loop += 1

            if next_seq == total_size and total_size not in window_packets:
                eof_packet = self.create_packet(total_size, EOF_MARKER)
                self.socket.sendto(eof_packet, client_addr)
                window_packets[total_size] = (eof_packet, time.time(), 0)
                total_packets_sent += 1

            in_flight = next_seq - base

            ready = select.select([self.socket], [], [], pacer_interval)

            if ready[0]:
                for _ in range(10):
                    try:
                        ack_packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                        recv_time = time.time()

                        ack_num, sack_blocks = self.parse_ack(ack_packet)

                        if ack_num is not None:
                            if ack_num > base:
                                acked_bytes = ack_num - base

                                if base in window_packets:
                                    _, send_time, retrans_count = window_packets[base]
                                    if retrans_count == 0:
                                        sample_rtt = recv_time - send_time
                                        self.calculate_rto(sample_rtt)

                                calculated_rto = (self.srtt + 4 * self.rttvar) if self.srtt else MIN_TIMEOUT
                                if self.rto > calculated_rto * 1.5:
                                    self.rto = max(self.rto * 0.9, calculated_rto)

                                self.cubic_update(acked_bytes)

                                for seq in list(window_packets.keys()):
                                    if seq < ack_num:
                                        del window_packets[seq]

                                base = ack_num
                                self.last_ack = ack_num
                                self.dup_ack_count = 0

                            elif ack_num == self.last_ack and ack_num > 0:
                                if next_seq > ack_num:
                                    self.dup_ack_count += 1

                                    if self.dup_ack_count == 3:
                                        self.on_congestion_event()
                                        self.log_state(event="fast_retransmit", extra_info=f"dup_acks=3, seq={base}")

                                        if base in window_packets:
                                            packet, _, retrans_count = window_packets[base]
                                            self.socket.sendto(packet, client_addr)
                                            window_packets[base] = (packet, time.time(), retrans_count + 1)
                                            retransmissions += 1

                                        self.dup_ack_count = 0

                            if sack_blocks:
                                now = time.time()
                                for start, end in sack_blocks:
                                    for seq in list(window_packets.keys()):
                                        if start <= seq < end and seq != base:
                                            del window_packets[seq]

                                if base in window_packets and sack_blocks:
                                    max_sack_end = max(end for _, end in sack_blocks)
                                    min_retrans_interval = self.srtt if self.srtt else 0.05
                                    retrans_this_ack = 0
                                    max_retrans_per_ack = 10

                                    for seq in list(window_packets.keys()):
                                        if base < seq < max_sack_end and retrans_this_ack < max_retrans_per_ack:
                                            is_sacked = any(start <= seq < end for start, end in sack_blocks)
                                            if not is_sacked:
                                                packet, send_time, retrans_count = window_packets[seq]
                                                if now - send_time > min_retrans_interval:
                                                    self.socket.sendto(packet, client_addr)
                                                    window_packets[seq] = (packet, now, retrans_count + 1)
                                                    retransmissions += 1
                                                    retrans_this_ack += 1
                    except socket.error:
                        break

            if window_packets and base in window_packets:
                current_time = time.time()
                packet, send_time, retrans_count = window_packets[base]
                if current_time - send_time > self.rto:
                    self.on_congestion_event(is_timeout=True)
                    self.log_state(event="timeout", extra_info=f"seq={base}, rto={self.rto:.3f}s")

                    self.socket.sendto(packet, client_addr)
                    window_packets[base] = (packet, current_time, retrans_count + 1)
                    retransmissions += 1

                    self.rto = min(MAX_TIMEOUT, self.rto * TIMEOUT_MULTIPLIER)

            current_time = time.time()
            if current_time - self.last_log_time >= 0.1:
                self.log_state(event="periodic")
                self.last_log_time = current_time

        self.log_state(event="transfer_complete", extra_info=f"packets={total_packets_sent}, retrans={retransmissions}")

        self.save_logs('cubic_logs_optimized.csv')
        transfer_time = time.time() - self.start_time
        avg_throughput = self.bytes_acked / transfer_time if transfer_time > 0 else 0
        print(f"\n=== Transfer Complete ===")
        print(f"Total packets: {total_packets_sent}, Retransmissions: {retransmissions}")
        print(f"Transfer time: {transfer_time:.2f}s")
        print(f"Bytes sent: {self.bytes_sent}, Bytes acked: {self.bytes_acked}")
        print(f"Average throughput: {avg_throughput/1024:.2f} KB/s ({avg_throughput*8/1000000:.2f} Mbps)")
        print(f"Final cwnd: {self.cwnd:.2f} bytes ({self.cwnd/MAX_DATA_SIZE:.2f} MSS)")
        print(f"Retransmission rate: {retransmissions/total_packets_sent*100:.2f}%")
        print(f"Logs saved to cubic_logs_optimized.csv")

    def run(self):
        print(f"Server listening on {self.server_ip}:{self.server_port} with TCP CUBIC")

        try:
            _, client_addr = self.socket.recvfrom(MAX_PACKET_SIZE)
            print(f"Received request from {client_addr}")

            self.send_file(client_addr)

        except socket.timeout:
            print("No client request received")
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.socket.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 p2_server_optimized.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])

    server = ReliableUDPServer(server_ip, server_port)
    server.run()

if __name__ == "__main__":
    main()