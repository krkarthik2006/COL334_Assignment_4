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
INITIAL_TIMEOUT = 0.05
TIMEOUT_MULTIPLIER = 1.5
MAX_TIMEOUT = 1.0
MIN_TIMEOUT = 0.05
SEND_BATCH_SIZE = 100  # Send packets in batches for better pipelining

class ReliableUDPServer:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Increase socket buffer sizes for better performance
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)  # 4MB send buffer
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)  # 4MB receive buffer
        except:
            pass

        self.socket.bind((server_ip, server_port))
        self.socket.settimeout(30.0)  # Initial timeout for receiving request

        # RTT estimation variables (using TCP-style EWMA)
        self.srtt = None  # Smoothed RTT
        self.rttvar = None  # RTT variance
        self.rto = INITIAL_TIMEOUT  # Retransmission timeout

        # TCP CUBIC congestion control state
        self.cwnd = 10 * MAX_DATA_SIZE  # Start at 10 MSS (modern TCP initial window)
        self.ssthresh = 500 * MAX_DATA_SIZE  # Initial threshold (~2.5x BDP for 100Mbps/40ms)

        # CUBIC specific variables
        self.W_max = 0  # Window size before last reduction
        self.K = 0  # Time period to reach W_max
        self.epoch_start = None  # Time of last congestion event
        self.tcp_cwnd = 10 * MAX_DATA_SIZE  # For TCP-friendly comparison
        self.acked_bytes_count = 0  # Track ACKs for TCP-friendly mode

        # CUBIC constants - tuned for 40ms RTT network
        self.C = 1.0  # Scaling factor (increased from 0.4 for faster growth)
        self.beta = 0.5  # Multiplicative decrease factor (TCP Reno-like for faster recovery)
        self.tcp_friendliness = True  # Enable hybrid mode

        # Duplicate ACK tracking for fast retransmit
        self.last_ack = 0
        self.dup_ack_count = 0

        # Track when we last grew cwnd (for RTT-based growth in CA)
        self.last_cwnd_growth_time = time.time()

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
            prev_rttvar = float(self.rttvar) if self.rttvar is not None else (sample_rtt / 2)
            self.rttvar = (1 - beta) * prev_rttvar + beta * abs(self.srtt - sample_rtt)
            self.srtt = (1 - alpha) * self.srtt + alpha * sample_rtt

        # Calculate RTO with bounds - more aggressive for better throughput
        self.rto = self.srtt + 4 * self.rttvar
        if self.rto < MIN_TIMEOUT:
            self.rto = MIN_TIMEOUT
        elif self.rto > MAX_TIMEOUT:
            self.rto = MAX_TIMEOUT

    def cubic_update(self, acked_bytes):
        """Update cwnd using CUBIC algorithm - optimized for 40ms RTT"""

        # Slow start phase - exponential growth
        if self.cwnd < self.ssthresh:
            self.cwnd += acked_bytes
            # Keep tcp_cwnd in sync during slow start
            self.tcp_cwnd = self.cwnd
            return

        # Congestion avoidance - first time entering, initialize tcp_cwnd
        if self.epoch_start is None:
            self.epoch_start = time.time()
            # Ensure tcp_cwnd is initialized to current cwnd
            if self.tcp_cwnd < self.cwnd:
                self.tcp_cwnd = self.cwnd

        t = time.time() - self.epoch_start  # Time since last congestion

        # Always calculate TCP Reno rate (for fairness)
        self.acked_bytes_count += acked_bytes
        if self.acked_bytes_count >= self.cwnd:
            # TCP Reno: increase by 1 MSS per RTT
            self.tcp_cwnd += MAX_DATA_SIZE
            self.acked_bytes_count -= self.cwnd

        # If no congestion yet, just use TCP Reno growth
        if self.W_max == 0:
            self.cwnd = self.tcp_cwnd
            return

        # CUBIC: W(t) = C * (t - K)^3 + W_max
        target = self.C * ((t - self.K) ** 3) + self.W_max

        # Use max of CUBIC and TCP-friendly (ensures fairness with Reno)
        if self.tcp_cwnd > target:
            self.cwnd = self.tcp_cwnd
        else:
            # Grow aggressively toward target
            if target > self.cwnd:
                # Calculate per-ACK increment
                # More aggressive: grow by a fraction of the gap each RTT
                gap = target - self.cwnd
                increment = max(gap / 10.0, MAX_DATA_SIZE) / self.cwnd * acked_bytes
                self.cwnd += increment
            else:
                # Near or past W_max, use standard additive increase
                self.cwnd = self.tcp_cwnd

        # Ensure cwnd stays reasonable
        self.cwnd = max(self.cwnd, MAX_DATA_SIZE)


    def on_congestion_event(self):
        """Handle congestion (timeout or 3 dup ACKs)"""

        # Save current window as W_max
        self.W_max = self.cwnd

        # Multiplicative decrease by beta (0.5 = TCP Reno behavior)
        self.cwnd = max(int(self.cwnd * self.beta), 2 * MAX_DATA_SIZE)
        self.ssthresh = self.cwnd

        # Reset CUBIC epoch - calculate K (time to reach W_max)
        if self.W_max > self.cwnd:
            self.K = ((self.W_max - self.cwnd) / self.C) ** (1/3)
        else:
            self.K = 0
        self.epoch_start = None

        # Reset TCP-friendly tracking to current cwnd
        self.tcp_cwnd = self.cwnd
        self.acked_bytes_count = 0

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
        """Send file using sliding window protocol with CUBIC congestion control"""
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

        # Set socket to non-blocking for better control
        self.socket.setblocking(False)

        # Track statistics
        total_packets_sent = 0
        retransmissions = 0

        # Initialize last_ack for duplicate ACK detection
        self.last_ack = 0
        self.dup_ack_count = 0

        while base <= total_size:
            # Send new packets while window allows
            packets_sent_in_batch = 0

            # Calculate in-flight bytes
            in_flight = next_seq - base

            # Send as much as cwnd allows, but pause periodically to check ACKs
            while next_seq < total_size and in_flight < self.cwnd:
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
                packets_sent_in_batch += 1

                # Update in-flight bytes
                in_flight = next_seq - base

                # Only pause to check ACKs after sending a batch
                # This prevents CPU monopolization while maintaining high throughput
                if packets_sent_in_batch >= SEND_BATCH_SIZE:
                    break

            # Send EOF packet after all data is sent
            if next_seq == total_size and total_size not in window_packets:
                eof_packet = self.create_packet(total_size, EOF_MARKER)
                self.socket.sendto(eof_packet, client_addr)
                window_packets[total_size] = (eof_packet, time.time(), 0)
                total_packets_sent += 1

            # Calculate in-flight bytes
            in_flight = next_seq - base

            # Determine if we should wait for ACKs or continue sending
            # Only wait if: (1) window is full OR (2) all data sent
            window_full = in_flight >= self.cwnd
            all_data_sent = next_seq >= total_size

            if window_full or all_data_sent:
                # Window is full or nothing more to send - wait for ACKs
                timeout = 0.001  # Default short timeout

                if base in window_packets:
                    # Get the send time of the OLDEST unacked packet
                    base_send_time = window_packets[base][1]
                    elapsed = time.time() - base_send_time
                    # Calculate time remaining until timeout
                    timeout = max(0.001, self.rto - elapsed)

                ready = select.select([self.socket], [], [], timeout)
            else:
                # Window has space and more data to send - check for ACKs without blocking
                ready = select.select([self.socket], [], [], 0.0001)  # 0.1ms timeout

            if ready[0]:
                # Receive all available ACKs (non-blocking)
                for _ in range(100):  # Process up to 100 ACKs per iteration
                    try:
                        ack_packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                        recv_time = time.time()

                        ack_num, sack_blocks = self.parse_ack(ack_packet)

                        if ack_num is not None:
                            # Handle cumulative ACK
                            if ack_num > base:
                                # New ACK received - slide window
                                acked_bytes = ack_num - base

                                # Update RTT estimation if this ACK acknowledges new data
                                # Karn's Algorithm: only use RTT from non-retransmitted packets
                                if base in window_packets:
                                    _, send_time, retrans_count = window_packets[base]
                                    if retrans_count == 0:  # Only use original transmissions
                                        sample_rtt = recv_time - send_time
                                        self.calculate_rto(sample_rtt)

                                # Update CUBIC window
                                self.cubic_update(acked_bytes)

                                # Remove ACKed packets from window
                                for seq in list(window_packets.keys()):
                                    if seq < ack_num:
                                        del window_packets[seq]

                                base = ack_num
                                self.last_ack = ack_num
                                self.dup_ack_count = 0

                            elif ack_num == self.last_ack and ack_num > 0:
                                # Duplicate ACK - increment counter
                                self.dup_ack_count += 1

                                # Fast retransmit on 3 duplicate ACKs
                                if self.dup_ack_count == 3:
                                    # Congestion event
                                    self.on_congestion_event()

                                    # Retransmit lost packet
                                    if base in window_packets:
                                        packet, _, retrans_count = window_packets[base]
                                        self.socket.sendto(packet, client_addr)
                                        window_packets[base] = (packet, time.time(), retrans_count + 1)
                                        retransmissions += 1

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
                        # No more ACKs available
                        break

            # Handle timeout - retransmit oldest unacknowledged packet
            if window_packets:
                current_time = time.time()
                # Only check the base packet for timeout (Go-Back-N style)
                if base in window_packets:
                    packet, send_time, retrans_count = window_packets[base]
                    if current_time - send_time > self.rto:
                        # Congestion event
                        self.on_congestion_event()

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
        print(f"Final cwnd: {self.cwnd:.2f} bytes ({self.cwnd/MAX_DATA_SIZE:.2f} MSS)")

    def run(self):
        """Main server loop"""
        print(f"Server listening on {self.server_ip}:{self.server_port} with TCP CUBIC")

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
            import traceback
            traceback.print_exc()
        finally:
            self.socket.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 p2_server.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])

    server = ReliableUDPServer(server_ip, server_port)
    server.run()

if __name__ == "__main__":
    main()
