import socket
import sys
import struct
import time
import select

# Constants
MAX_PACKET_SIZE = 1200
HEADER_SIZE = 20
REQUEST_TIMEOUT = 2.0
MAX_REQUEST_RETRIES = 5
EOF_MARKER = b"EOF"
ACK_INTERVAL = 0.005  # Send ACKs very quickly
DELAYED_ACK_THRESHOLD = 1  # ACK every packet for maximum throughput

class ReliableUDPClient:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Increase socket buffer sizes for better performance
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB send buffer
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB receive buffer
        except:
            pass

        self.socket.settimeout(REQUEST_TIMEOUT)

        # Receiver state
        self.received_data = {}  # {seq_num: data}
        self.next_expected = 0  # Next expected sequence number (for cumulative ACK)
        self.last_ack_sent_time = 0
        self.packets_since_ack = 0  # Counter for delayed ACK optimization

    def parse_packet(self, packet):
        """Parse received packet to extract sequence number and data"""
        if len(packet) < HEADER_SIZE:
            return None, None

        seq_num = struct.unpack('!I', packet[:4])[0]
        data = packet[HEADER_SIZE:]
        return seq_num, data

    def create_ack(self, ack_num, sack_blocks=None):
        """Create ACK packet with cumulative ACK and optional SACK blocks"""
        # ACK format: [ack_num (4 bytes)][SACK blocks in reserved 16 bytes][no data]
        ack_packet = struct.pack('!I', ack_num)

        # Add SACK blocks if present (up to 2 blocks in 16 bytes)
        if sack_blocks and len(sack_blocks) > 0:
            sack_data = b''
            for start, end in sack_blocks[:2]:  # Max 2 SACK blocks
                sack_data += struct.pack('!II', start, end)
            # Pad to 16 bytes
            sack_data += b'\x00' * (16 - len(sack_data))
            ack_packet += sack_data
        else:
            ack_packet += b'\x00' * 16

        return ack_packet

    def get_sack_blocks(self):
        """Generate SACK blocks for out-of-order received data"""
        if not self.received_data:
            return []

        # Find contiguous blocks of received data beyond next_expected
        sack_blocks = []
        sorted_seqs = sorted(self.received_data.keys())

        i = 0
        while i < len(sorted_seqs):
            seq = sorted_seqs[i]
            if seq <= self.next_expected:
                i += 1
                continue

            # Start of a SACK block
            block_start = seq
            block_end = seq + len(self.received_data[seq])

            # Extend block if contiguous packets follow
            j = i + 1
            while j < len(sorted_seqs):
                next_seq = sorted_seqs[j]
                if next_seq <= block_end:
                    # Extend block
                    block_end = max(block_end, next_seq + len(self.received_data[next_seq]))
                    j += 1
                else:
                    break

            sack_blocks.append((block_start, block_end))
            i = j

        return sack_blocks

    def send_ack(self, force=False):
        """Send ACK with cumulative ACK number and SACK blocks"""
        sack_blocks = self.get_sack_blocks()
        ack_packet = self.create_ack(self.next_expected, sack_blocks)
        self.socket.sendto(ack_packet, (self.server_ip, self.server_port))
        self.last_ack_sent_time = time.time()
        self.packets_since_ack = 0

    def request_file(self):
        """Send file request to server with retries"""
        request_msg = b'R'  # 1-byte request

        for attempt in range(MAX_REQUEST_RETRIES):
            try:
                self.socket.sendto(request_msg, (self.server_ip, self.server_port))
                print(f"Sent file request (attempt {attempt + 1}/{MAX_REQUEST_RETRIES})")

                # Wait for first data packet as confirmation
                ready = select.select([self.socket], [], [], REQUEST_TIMEOUT)
                if ready[0]:
                    # Received response
                    return True

            except socket.timeout:
                print(f"Request timeout (attempt {attempt + 1}/{MAX_REQUEST_RETRIES})")

        print("Failed to connect to server after maximum retries")
        return False

    def receive_file(self):
        """Receive file from server using sliding window protocol"""
        # Send initial request
        if not self.request_file():
            return False

        # Set socket to non-blocking for better control
        self.socket.setblocking(False)

        # Statistics
        packets_received = 0
        total_bytes = 0
        eof_received = False
        last_activity = time.time()
        stall_timeout = 3.0  # If no packets for 3 seconds, assume done

        print("Receiving file...")

        while not eof_received:
            # Check for timeout (no activity)
            if time.time() - last_activity > stall_timeout:
                print("Connection stalled, finishing...")
                break

            # Receive packets - very short timeout for better responsiveness
            ready = select.select([self.socket], [], [], 0.01)

            if ready[0]:
                try:
                    packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                    last_activity = time.time()

                    seq_num, data = self.parse_packet(packet)

                    if seq_num is not None and data is not None:
                        # Check for EOF marker
                        if data == EOF_MARKER:
                            print("Received EOF marker")
                            eof_received = True
                            # Send final ACK
                            self.send_ack()
                            break

                        # Store received data
                        if seq_num not in self.received_data:
                            self.received_data[seq_num] = data
                            packets_received += 1
                            total_bytes += len(data)
                            self.packets_since_ack += 1

                            # Update next_expected if we received the next in-order packet
                            while self.next_expected in self.received_data:
                                self.next_expected += len(self.received_data[self.next_expected])

                            # ACK every packet immediately for best throughput in lossy conditions
                            # With 1-5% loss, aggressive ACKing helps server advance window faster
                            self.send_ack()
                        else:
                            # Duplicate packet - send ACK immediately (helps server)
                            self.send_ack()

                except socket.error:
                    pass

            # Send periodic ACKs even if no new packets (duplicate ACKs for reliability)
            # Very fast periodic ACKs for low latency
            current_time = time.time()
            if current_time - self.last_ack_sent_time >= 0.01:  # At least every 10ms
                self.send_ack()

        # Send final ACKs to ensure server knows we're done - minimal overhead
        self.send_ack()
        time.sleep(0.01)
        self.send_ack()

        print(f"Received {packets_received} packets, {total_bytes} bytes")

        # Write received data to file in order
        return self.write_file()

    def write_file(self):
        """Write received data to file in correct order"""
        try:
            with open('received_data.txt', 'wb') as f:
                # Write data in sequence number order
                current_seq = 0
                sorted_seqs = sorted(self.received_data.keys())

                for seq in sorted_seqs:
                    if seq == current_seq:
                        data = self.received_data[seq]
                        f.write(data)
                        current_seq += len(data)
                    elif seq > current_seq:
                        # Gap detected - this shouldn't happen if protocol works correctly
                        print(f"Warning: Gap detected at sequence {current_seq}, next is {seq}")
                        # Try to continue anyway
                        current_seq = seq
                        data = self.received_data[seq]
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
