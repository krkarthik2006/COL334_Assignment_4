import socket
import sys
import struct
import time
import select

# Constants
MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
MAX_DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
EOF_MARKER = b"EOF"
INITIAL_TIMEOUT = 0.15
TIMEOUT_MULTIPLIER = 1.2
MAX_TIMEOUT = 0.8
MIN_TIMEOUT = 0.025

class ReliableUDPServer:
    def __init__(self, server_ip, server_port, sws):
        self.server_ip = server_ip
        self.server_port = server_port
        self.sws = sws  # Sender Window Size in bytes
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Increase socket buffer sizes for better performance
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB send buffer
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB receive buffer
        except:
            pass

        self.socket.bind((server_ip, server_port))
        self.socket.settimeout(30.0)  # Initial timeout for receiving request

        # RTT estimation variables (using TCP-style EWMA)
        self.srtt = None  # Smoothed RTT
        self.rttvar = None  # RTT variance
        self.rto = INITIAL_TIMEOUT  # Retransmission timeout

    def calculate_rto(self, sample_rtt):
        """Calculate RTO using TCP-style exponential weighted moving average"""
        alpha = 0.125
        beta = 0.25

        if self.srtt is None:
            # First measurement
            self.srtt = sample_rtt
            self.rttvar = sample_rtt / 2
        else:
            # Update SRTT and RTTVAR
            self.rttvar = (1 - beta) * self.rttvar + beta * abs(self.srtt - sample_rtt)
            self.srtt = (1 - alpha) * self.srtt + alpha * sample_rtt

        # Calculate RTO with bounds - more aggressive for better throughput
        self.rto = self.srtt + 4 * self.rttvar  # Simplified, TCP uses 4*rttvar
        if self.rto < MIN_TIMEOUT:
            self.rto = MIN_TIMEOUT
        elif self.rto > MAX_TIMEOUT:
            self.rto = MAX_TIMEOUT

    def create_packet(self, seq_num, data):
        """Create a packet with sequence number and data"""
        # Packet format: [seq_num (4 bytes)][reserved (16 bytes)][data (up to 1180 bytes)]
        header = struct.pack('!I', seq_num) + b'\x00' * 16
        return header + data

    def parse_ack(self, packet):
        """Parse ACK packet to extract ACK number and optional SACK info"""
        if len(packet) < 4:
            return None, None

        ack_num = struct.unpack('!I', packet[:4])[0]

        # Check for SACK blocks in reserved space
        sack_blocks = []
        if len(packet) >= 20:
            # SACK format: up to 2 blocks of (start, end) in reserved 16 bytes
            reserved = packet[4:20]
            for i in range(0, 16, 8):
                if i + 8 <= len(reserved):
                    start, end = struct.unpack('!II', reserved[i:i+8])
                    if start > 0 and end > start:
                        sack_blocks.append((start, end))

        return ack_num, sack_blocks

    def send_file(self, client_addr):
        """Send file using sliding window protocol with advanced features"""
        # Get file size without reading entire file into memory
        try:
            import os
            total_size = os.path.getsize('data.txt')
            file_handle = open('data.txt', 'rb')
        except FileNotFoundError:
            print("Error: data.txt not found")
            return

        print(f"Sending file of size {total_size} bytes to {client_addr}")

        # Window management
        base = 0  # First unacknowledged byte
        next_seq = 0  # Next sequence number to send
        window_packets = {}  # {seq_num: (data, send_time, retransmit_count)}
        dup_ack_count = {}  # Track duplicate ACKs for fast retransmit
        last_ack_received = 0


        # Set socket to non-blocking for better control
        self.socket.setblocking(False)

        # Track statistics
        total_packets_sent = 0
        retransmissions = 0

        while base <= total_size:
            # Send new packets while window allows
            while next_seq < total_size and (next_seq - base) < self.sws:
                # Read chunk of data from file on-demand
                chunk_start = next_seq
                chunk_end = min(next_seq + MAX_DATA_SIZE, total_size)

                # Seek to position and read chunk
                file_handle.seek(chunk_start)
                data = file_handle.read(chunk_end - chunk_start)

                # Create and send packet
                packet = self.create_packet(next_seq, data)
                self.socket.sendto(packet, client_addr)

                # Store packet info for potential retransmission
                window_packets[next_seq] = (packet, time.time(), 0)
                next_seq = chunk_end
                total_packets_sent += 1

            # Send EOF packet after all data is sent
            if next_seq == total_size and total_size not in window_packets:
                eof_packet = self.create_packet(total_size, EOF_MARKER)
                self.socket.sendto(eof_packet, client_addr)
                window_packets[total_size] = (eof_packet, time.time(), 0)
                total_packets_sent += 1

            # Wait for ACKs with timeout - let OS handle waiting efficiently
            timeout = 0.01  # Default timeout if no packets in flight to check for ACKs

            if base in window_packets:
                # Get the send time of the OLDEST unacked packet (base)
                base_send_time = window_packets[base][1]
                elapsed = time.time() - base_send_time

                # Calculate time remaining until this packet times out
                timeout = max(0.001, self.rto - elapsed)

            ready = select.select([self.socket], [], [], timeout)

            if ready[0]:
                # Receive ACK
                try:
                    ack_packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                    recv_time = time.time()

                    ack_num, sack_blocks = self.parse_ack(ack_packet)

                    if ack_num is not None:
                        # Update RTT estimation if this ACK acknowledges new data
                        # Karn's Algorithm: only use RTT from non-retransmitted packets
                        if ack_num > base and base in window_packets:
                            _, send_time, retrans_count = window_packets[base]
                            if retrans_count == 0:  # Only use original transmissions
                                sample_rtt = recv_time - send_time
                                self.calculate_rto(sample_rtt)

                        # Handle cumulative ACK
                        if ack_num > base:
                            # New ACK received - slide window
                            for seq in list(window_packets.keys()):
                                if seq < ack_num:
                                    del window_packets[seq]
                            base = ack_num
                            last_ack_received = ack_num
                            dup_ack_count.clear()

                        elif ack_num == last_ack_received:
                            # Duplicate ACK - increment counter
                            dup_ack_count[ack_num] = dup_ack_count.get(ack_num, 0) + 1

                            # Fast retransmit on 3 duplicate ACKs
                            if dup_ack_count[ack_num] >= 3 and base in window_packets:
                                packet, _, retrans_count = window_packets[base]
                                self.socket.sendto(packet, client_addr)
                                window_packets[base] = (packet, time.time(), retrans_count + 1)
                                retransmissions += 1
                                # Don't change RTO on fast retransmit

                        # Handle SACK blocks if present - selective retransmission
                        if sack_blocks:
                            now = time.time()
                            # Remove SACKed packets from retransmission list
                            for start, end in sack_blocks:
                                for seq in list(window_packets.keys()):
                                    if start <= seq < end and seq != base:
                                        del window_packets[seq]

                            # Proactively retransmit gaps - all holes up to max SACK end
                            if base in window_packets and sack_blocks:
                                max_sack_end = max(end for _, end in sack_blocks)
                                # Find and retransmit ALL missing packets (not covered by SACK)
                                for seq in list(window_packets.keys()):
                                    if base < seq < max_sack_end:
                                        # Check if this seq is covered by any SACK block
                                        is_sacked = any(start <= seq < end for start, end in sack_blocks)
                                        if not is_sacked:
                                            packet, send_time, retrans_count = window_packets[seq]
                                            # Immediate retransmission of gaps
                                            self.socket.sendto(packet, client_addr)
                                            window_packets[seq] = (packet, now, retrans_count + 1)
                                            retransmissions += 1

                except socket.error:
                    pass

            # Handle timeout - retransmit oldest unacknowledged packet
            if window_packets:
                current_time = time.time()
                # Only check the base packet for timeout (Go-Back-N style)
                if base in window_packets:
                    packet, send_time, retrans_count = window_packets[base]
                    if current_time - send_time > self.rto:
                        # Retransmit packet
                        self.socket.sendto(packet, client_addr)
                        window_packets[base] = (packet, current_time, retrans_count + 1)
                        retransmissions += 1

                        # Mild exponential backoff
                        if retrans_count >= 2:
                            self.rto = min(MAX_TIMEOUT, self.rto * TIMEOUT_MULTIPLIER)

        # Close file handle
        file_handle.close()

        print(f"File transfer complete. Total packets: {total_packets_sent}, Retransmissions: {retransmissions}")

    def run(self):
        """Main server loop"""
        print(f"Server listening on {self.server_ip}:{self.server_port} with SWS={self.sws}")

        try:
            # Wait for client request
            _, client_addr = self.socket.recvfrom(MAX_PACKET_SIZE)
            print(f"Received request from {client_addr}")

            # Send file to client
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

