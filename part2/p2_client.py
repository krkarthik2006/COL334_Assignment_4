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
    def __init__(self, server_ip, server_port, pref_filename):
        self.server_ip = server_ip
        self.server_port = server_port
        self.pref_filename = pref_filename
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Increase socket buffer sizes for better performance
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8388608)  # 8MB send buffer
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8388608)  # 8MB receive buffer
        except:
            pass

        self.socket.settimeout(REQUEST_TIMEOUT)

        # Receiver state
        self.received_data = {}  # {seq_num: data} -> NOW ONLY FOR OUT-OF-ORDER
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
            if seq < self.next_expected: # Note: changed from <=
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
        """
        Receive file from server using sliding window protocol.
        Writes in-order data directly to disk.
        """
        if not self.request_file():
            return False

        self.socket.setblocking(False)

        packets_received = 0
        total_bytes = 0
        eof_received = False
        eof_seq_num = None
        last_activity = time.time()
        stall_timeout = 3.0 # If no packets for 3 seconds, assume done

        output_filename = f"{self.pref_filename}received_data.txt"

        try:
            with open(output_filename, 'wb') as file_handle:
                print("Receiving file...")

                while True:
                    if time.time() - last_activity > stall_timeout:
                        if eof_received and self.next_expected == eof_seq_num:
                            print("Stall timeout after EOF, transfer complete.")
                        else:
                            print(f"Connection stalled, timeout. next_expected={self.next_expected}, eof_seq_num={eof_seq_num}")
                        break # Exit main loop on stall

                    ready = select.select([self.socket], [], [], 0.01)

                    if not ready[0]:
                        # No packets, check if we need to send a periodic ACK
                        current_time = time.time()
                        if current_time - self.last_ack_sent_time >= 0.05: # At least every 50ms
                            self.send_ack()
                        continue

                    # Packets are ready
                    try:
                        packet, _ = self.socket.recvfrom(MAX_PACKET_SIZE)
                        last_activity = time.time()
                        seq_num, data = self.parse_packet(packet)

                        if seq_num is None or data is None:
                            continue # Corrupt packet

                        # --- EOF Packet Logic ---
                        if data == EOF_MARKER:
                            print(f"Received EOF marker at sequence {seq_num}")
                            eof_received = True
                            eof_seq_num = seq_num
                            if seq_num == self.next_expected:
                                # EOF arrived perfectly in order. We are done.
                                print("EOF received in order. Finishing.")
                                self.send_ack() # Send final ACK
                                break # Exit main loop
                            else:
                                # EOF arrived out of order. Send ACK and wait for missing data.
                                self.send_ack()
                                continue

                        # --- Regular Data Packet Logic ---
                        
                        # 1. If packet is out of order (in the future)
                        if seq_num > self.next_expected:
                            if seq_num not in self.received_data:
                                # Buffer out-of-order packet
                                self.received_data[seq_num] = data
                                packets_received += 1
                                total_bytes += len(data)
                            self.send_ack() # Send SACK for this packet
                            continue

                        # 2. If packet is in-order (seq_num == self.next_expected)
                        if seq_num == self.next_expected:
                            # Write this packet's data directly to disk
                            file_handle.write(data)
                            self.next_expected += len(data)
                            packets_received += 1
                            total_bytes += len(data)

                            # 3. Check buffer for contiguous packets
                            # This loop "unlocks" buffered packets
                            while self.next_expected in self.received_data:
                                buffered_data = self.received_data.pop(self.next_expected)
                                file_handle.write(buffered_data)
                                self.next_expected += len(buffered_data)
                            
                            self.send_ack() # Send cumulative ACK

                            # 4. Check for completion
                            if eof_received and self.next_expected == eof_seq_num:
                                print("All data received after EOF, finishing...")
                                break # Exit main loop
                        
                        # 5. If packet is a duplicate (in the past)
                        elif seq_num < self.next_expected:
                            self.send_ack() # Re-send last cumulative ACK

                    except socket.error:
                        pass # No more packets to read for now

        except Exception as e:
            print(f"Error during file receive or write: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # After loop breaks (finish or stall)
        print(f"Received {packets_received} packets, {total_bytes} bytes")
        print(f"File written to {output_filename} ({self.next_expected} bytes)")

        # Send final ACKs to ensure server knows we're done
        self.send_ack()
        time.sleep(0.01)
        self.send_ack()
        
        # Check if we finished successfully
        if eof_received and self.next_expected == eof_seq_num:
            return True
        else:
            print(f"Transfer incomplete. EOF received: {eof_received}. Next expected: {self.next_expected}. EOF sequence: {eof_seq_num}")
            return False

    def write_file(self):
        """
        This function is no longer used.
        File writing is now handled directly in receive_file().
        """
        print("Note: write_file() is deprecated.")
        pass

    def run(self):
        """Main client execution"""
        print(f"Connecting to server at {self.server_ip}:{self.server_port}")
        success = False
        try:
            success = self.receive_file()
            if success:
                print("File transfer completed successfully")
            else:
                print("File transfer failed or was incomplete")

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.socket.close()
            print("Client socket closed. Exiting.")
            # The script will now naturally exit, and p2_exp.py will detect it

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 p2_client.py <SERVER_IP> <SERVER_PORT> <PREF_FILENAME>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    pref_filename = sys.argv[3]

    client = ReliableUDPClient(server_ip, server_port, pref_filename)
    client.run()

if __name__ == "__main__":
    main()