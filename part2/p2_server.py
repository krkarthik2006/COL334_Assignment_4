import socket
import sys
import struct
import time
import select
import csv
from collections import deque

# Constants
MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
MAX_DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
EOF_MARKER = b"EOF"
INITIAL_TIMEOUT = 0.2
TIMEOUT_MULTIPLIER = 2.0
MAX_TIMEOUT = 3.0  # Allow proper exponential backoff
MIN_TIMEOUT = 0.1
MAX_CWND = 1000 * MAX_DATA_SIZE  # Cap cwnd at 500 MSS to prevent overwhelming network

MSS = MAX_DATA_SIZE

class ReliableUDPServer:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.MSS = MSS
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
        self.cwnd = float(1 * MSS)  # Start with 1 MSS acc to assgn constraints
        self.ssthresh = float(16 * MSS)  # Target: reach 64 MSS in slow start, then CUBIC takes over

        # CUBIC specific state
        self.W_max = 0  # Window size at last congestion event (in MSS units)
        self.K = 0  # Time period to reach W_max again (in seconds)
        self.epoch_start = None  # Time when current epoch started
        self.tcp_cwnd = float(self.cwnd)  # TCP-friendly window estimate

        # CUBIC parameters (RFC 8312)
        self.C = 0.4  # CUBIC scaling constant
        self.beta = 0.7  # Multiplicative decrease factor (CUBIC uses 0.7, TCP uses 0.5)

        # Duplicate ACK tracking
        self.last_ack = 0
        self.dup_ack_count = 0
        self.last_congestion_time = 0  # Prevent rapid congestion events

        # Logging and statistics
        self.start_time = None
        self.log_data = []
        self.bytes_sent = 0
        self.bytes_acked = 0
        self.sending_rate_window = deque(maxlen=10)  # Track last 10 measurements
        self.last_log_time = None

        # No pacing - send as fast as cwnd allows

    def log_state(self, event="periodic", extra_info=None):
        """
        Log current congestion control state for analysis.
        Records: time, cwnd, ssthresh, mode, event, sending_rate, throughput
        """
        if self.start_time is None:
            return

        current_time = time.time() - self.start_time

        # Determine mode
        if self.cwnd < self.ssthresh:
            mode = "slow_start"
        else:
            mode = "congestion_avoidance"

        # Calculate sending rate (bytes/sec over recent window)
        sending_rate = 0
        if len(self.sending_rate_window) > 0:
            total_sent = sum(b for b, _ in self.sending_rate_window)
            time_span = self.sending_rate_window[-1][1] - self.sending_rate_window[0][1]
            if time_span > 0:
                sending_rate = total_sent / time_span

        # Calculate throughput (acked bytes / time)
        throughput = self.bytes_acked / current_time if current_time > 0 else 0

        # Log entry
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
        """Save logged data to CSV file for analysis."""
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

        # Calculate RTO per RFC 6298
        self.rto = self.srtt + 4 * self.rttvar
        if self.rto < MIN_TIMEOUT:
            self.rto = MIN_TIMEOUT
        elif self.rto > MAX_TIMEOUT:
            self.rto = MAX_TIMEOUT

    def cubic_update(self, acked_bytes):
        """
        Update cwnd using CUBIC algorithm per RFC 8312.
        Called when new data is ACKed.
        """
        if acked_bytes <= 0:
            return

        # Track acked bytes
        self.bytes_acked += acked_bytes

        # Slow start: controlled growth (slower than standard TCP)
        if self.cwnd < self.ssthresh:
            # Slower growth: increase by 50% per RTT instead of doubling
            # This gives more controlled ramp-up to avoid overwhelming network
            old_cwnd = self.cwnd
            self.cwnd += acked_bytes * 0.5  # 50% increase per RTT
            self.tcp_cwnd = self.cwnd

            # Cap cwnd at maximum
            if self.cwnd > MAX_CWND:
                self.cwnd = MAX_CWND
                self.tcp_cwnd = self.cwnd

            # Log if significant change (crossed MSS boundary)
            if int(old_cwnd / MSS) < int(self.cwnd / MSS):
                self.log_state(event="slow_start_growth", extra_info=f"cwnd: {old_cwnd:.0f} -> {self.cwnd:.0f}")
            return
        
        if self.W_max == 0:
            # Reno's Additive Increase: increment by ~1 MSS per RTT
            tcp_increment = acked_bytes * MSS / max(self.cwnd, MSS)
            self.cwnd += tcp_increment
            self.tcp_cwnd = self.cwnd  # Keep tcp_cwnd in sync

            # Enforce window size limits
            if self.cwnd > MAX_CWND:
                self.cwnd = MAX_CWND
                self.tcp_cwnd = self.cwnd
            
            return

        # Congestion avoidance: CUBIC growth
        # Start a new epoch if needed
        if self.epoch_start is None:
            self.epoch_start = time.time()
            # If no prior W_max (first time in CA), set to current window
            if self.W_max == 0:
                self.W_max = self.cwnd / MSS
            # Calculate K: time to reach W_max
            self.K = ((self.W_max * (1.0 - self.beta)) / self.C) ** (1.0 / 3.0)
            # Initialize TCP-friendly window
            self.tcp_cwnd = self.cwnd
            # Log epoch start
            self.log_state(event="epoch_start", extra_info=f"W_max={self.W_max:.2f}, K={self.K:.2f}s")

        # Time since epoch start
        t = time.time() - self.epoch_start

        # CUBIC window: W_cubic(t) = C * (t - K)^3 + W_max (in MSS units)
        target_mss = self.C * ((t - self.K) ** 3) + self.W_max
        target_cwnd = target_mss * MSS  # convert to bytes

        # TCP-friendly window (Reno estimate)
        # Increment per ACK in CA: alpha * MSS / cwnd where alpha ~= 1
        tcp_increment = acked_bytes * MSS / max(self.tcp_cwnd, MSS)
        self.tcp_cwnd += tcp_increment

        # CUBIC increment calculation
        if target_cwnd > self.cwnd:
            # Below target: ramp up using CUBIC
            # cnt approximates how many ACKs needed to close the gap
            cnt = self.cwnd / (target_cwnd - self.cwnd)
            if cnt < 1:
                cnt = 1
            increment = acked_bytes / cnt
        else:
            # At or above target: use small additive increase
            increment = acked_bytes * MSS / self.cwnd

        # Use max(W_cubic, W_tcp) for TCP-friendliness
        cwnd_cubic = self.cwnd + increment
        if self.tcp_cwnd > cwnd_cubic:
            self.cwnd = self.tcp_cwnd
        else:
            self.cwnd = cwnd_cubic

        # Enforce window size limits
        if self.cwnd < MSS:
            self.cwnd = MSS
        elif self.cwnd > MAX_CWND:
            self.cwnd = MAX_CWND
            self.tcp_cwnd = self.cwnd



    def on_congestion_event(self, is_timeout=False):
        """
        Handle congestion event (packet loss) per RFC 8312.
        Reduces window and records W_max.
        is_timeout: If True, indicates timeout (more severe) vs fast retransmit
        """
        # Prevent multiple congestion responses within one RTT
        current_time = time.time()
        min_interval = self.srtt if self.srtt else 0.05  # Use estimated RTT or 50ms

        if current_time - self.last_congestion_time < min_interval:
            # Too soon after last congestion event, ignore
            return

        self.last_congestion_time = current_time

        # Record window size before reduction (in MSS units)
        old_cwnd = self.cwnd
        self.W_max = self.cwnd / MSS

        # Multiplicative decrease by beta
        new_cwnd = max(self.cwnd * self.beta, 2 * MSS)

        # Set ssthresh to the reduced window
        self.ssthresh = max(new_cwnd, 2 * MSS)

        # For timeout (severe congestion), reduce cwnd more to re-enter slow start
        if is_timeout:
            self.cwnd = max(new_cwnd * 0.5, 2 * MSS)  # Drop to 50% to ensure slow start
        else:
            self.cwnd = new_cwnd  # Fast retransmit: stay at same level

        # Calculate K: time to grow back to W_max
        # K = cubic_root((W_max - W_max*beta) / C) = cubic_root((W_max * (1-beta)) / C)
        if self.W_max > 0:
            self.K = ((self.W_max * (1.0 - self.beta)) / self.C) ** (1.0 / 3.0)
        else:
            self.K = 0.0

        # Start new epoch on next ACK
        self.epoch_start = None
        self.tcp_cwnd = self.cwnd

        # Log congestion event
        self.log_state(event="congestion", extra_info=f"cwnd: {old_cwnd:.0f} -> {self.cwnd:.0f}, ssthresh: {self.ssthresh:.0f}")


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
        """Send file using sliding window protocol with TCP CUBIC congestion control"""
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

        # Initialize logging
        self.start_time = time.time()
        self.bytes_sent = 0
        self.bytes_acked = 0
        self.last_log_time = self.start_time
        self.log_state(event="transfer_start", extra_info=f"file_size={total_size}")
        max_burst_packets = 3
        pacer_timeout = 0.00113
        while base <= total_size:
            # Calculate in-flight bytes
            in_flight = next_seq - base
            packets_in_this_loop = 0
            # Send packets to fill the congestion window
            # No artificial delays - just controlled by cwnd growth rate
            while next_seq < total_size and in_flight < self.cwnd and packets_in_this_loop < max_burst_packets:
                current_time = time.time()

                # Read chunk of data from file on-demand
                chunk_start = next_seq
                chunk_end = min(next_seq + MAX_DATA_SIZE, total_size)

                file_handle.seek(chunk_start)
                data = file_handle.read(chunk_end - chunk_start)

                # Create and send packet
                packet = self.create_packet(next_seq, data)
                self.socket.sendto(packet, client_addr)

                # Track bytes sent
                bytes_in_packet = len(data)
                self.bytes_sent += bytes_in_packet
                self.sending_rate_window.append((bytes_in_packet, time.time()))

                # Store packet info
                window_packets[next_seq] = (packet, current_time, 0)
                next_seq = chunk_end
                total_packets_sent += 1

                # Update in-flight for next iteration
                in_flight = next_seq - base
                packets_in_this_loop += 1

            # Send EOF packet after all data is sent
            if next_seq == total_size and total_size not in window_packets:
                eof_packet = self.create_packet(total_size, EOF_MARKER)
                self.socket.sendto(eof_packet, client_addr)
                window_packets[total_size] = (eof_packet, time.time(), 0)
                total_packets_sent += 1

            # Calculate in-flight bytes
            in_flight = next_seq - base

            # Use balanced timeout for ACK processing
            # Balance between responsiveness and CPU usage
            ready = select.select([self.socket], [], [], pacer_timeout)  # 1ms timeout

            if ready[0]:
                # Receive all available ACKs
                for _ in range(100):
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

                                # On successful ACK, gradually reduce RTO if it's significantly backed off
                                # But don't do this too aggressively to avoid RTO oscillations
                                calculated_rto = (self.srtt + 4 * self.rttvar) if self.srtt else MIN_TIMEOUT
                                if self.rto > calculated_rto * 1.5:  # Only if RTO is 50% higher than calculated
                                    self.rto = max(self.rto * 0.9, calculated_rto)  # Reduce slowly

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
                                # Duplicate ACK - only count if we have unacked data beyond this point
                                # This prevents false positives from over-eager client ACKs
                                if next_seq > ack_num:
                                    self.dup_ack_count += 1

                                    # Fast retransmit on 3 duplicate ACKs
                                    if self.dup_ack_count == 3:
                                        # Congestion event
                                        self.on_congestion_event()
                                        self.log_state(event="fast_retransmit", extra_info=f"dup_acks=3, seq={base}")

                                        # Retransmit lost packet
                                        if base in window_packets:
                                            packet, _, retrans_count = window_packets[base]
                                            self.socket.sendto(packet, client_addr)
                                            window_packets[base] = (packet, time.time(), retrans_count + 1)
                                            retransmissions += 1

                                        # Reset dup_ack_count after fast retransmit
                                        self.dup_ack_count = 0

                            # Handle SACK blocks if present - selective retransmission
                            if sack_blocks:
                                now = time.time()
                                # Remove SACKed packets from retransmission list
                                for start, end in sack_blocks:
                                    for seq in list(window_packets.keys()):
                                        if start <= seq < end and seq != base:
                                            del window_packets[seq]

                                # Proactively retransmit gaps - all holes up to max SACK end
                                # But limit retransmissions to prevent storms
                                if base in window_packets and sack_blocks:
                                    max_sack_end = max(end for _, end in sack_blocks)
                                    min_retrans_interval = self.srtt if self.srtt else 0.05
                                    retrans_this_ack = 0
                                    max_retrans_per_ack = 5  # Limit retransmissions per ACK

                                    # Find and retransmit missing packets (not covered by SACK)
                                    for seq in list(window_packets.keys()):
                                        if base < seq < max_sack_end and retrans_this_ack < max_retrans_per_ack:
                                            # Check if this seq is covered by any SACK block
                                            is_sacked = any(start <= seq < end for start, end in sack_blocks)
                                            if not is_sacked:
                                                packet, send_time, retrans_count = window_packets[seq]
                                                # Only retransmit if enough time has passed since last send
                                                if now - send_time > min_retrans_interval:
                                                    self.socket.sendto(packet, client_addr)
                                                    window_packets[seq] = (packet, now, retrans_count + 1)
                                                    retransmissions += 1
                                                    retrans_this_ack += 1
                    except socket.error:
                        # No more ACKs available
                        break

            # Handle timeout - retransmit oldest unacknowledged packet
            if window_packets and base in window_packets:
                current_time = time.time()
                packet, send_time, retrans_count = window_packets[base]
                if current_time - send_time > self.rto:
                    # Congestion event (timeout is severe)
                    self.on_congestion_event(is_timeout=True)
                    self.log_state(event="timeout", extra_info=f"seq={base}, rto={self.rto:.3f}s")

                    # Retransmit packet
                    self.socket.sendto(packet, client_addr)
                    window_packets[base] = (packet, current_time, retrans_count + 1)
                    retransmissions += 1

                    # Exponential backoff on every timeout
                    self.rto = min(MAX_TIMEOUT, self.rto * TIMEOUT_MULTIPLIER)

            # Periodic logging (every 0.1 seconds)
            current_time = time.time()
            if current_time - self.last_log_time >= 0.1:
                self.log_state(event="periodic")
                self.last_log_time = current_time

        # Close file handle
        file_handle.close()

        # Log transfer completion
        self.log_state(event="transfer_complete", extra_info=f"packets={total_packets_sent}, retrans={retransmissions}")

        # Save logs to CSV
        self.save_logs('cubic_logs.csv')

        # Print statistics
        transfer_time = time.time() - self.start_time
        avg_throughput = self.bytes_acked / transfer_time if transfer_time > 0 else 0
        print(f"\n=== Transfer Complete ===")
        print(f"Total packets: {total_packets_sent}, Retransmissions: {retransmissions}")
        print(f"Transfer time: {transfer_time:.2f}s")
        print(f"Bytes sent: {self.bytes_sent}, Bytes acked: {self.bytes_acked}")
        print(f"Average throughput: {avg_throughput/1024:.2f} KB/s")
        print(f"Final cwnd: {self.cwnd:.2f} bytes ({self.cwnd/MAX_DATA_SIZE:.2f} MSS)")
        print(f"Logs saved to cubic_logs.csv")

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
