# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is COL334 Assignment 4: a UDP-based reliable file transfer protocol with congestion control. The project implements a client-server application where the client downloads a file from the server over UDP, with custom reliability and congestion control mechanisms at the application layer.

## Running the Code

### Server
```bash
python3 p1_server.py <SERVER_IP> <SERVER_PORT> <SWS>
```
- `<SERVER_IP>`: Server's IP address (e.g., 10.0.0.1)
- `<SERVER_PORT>`: Server's listening port (e.g., 6555)
- `<SWS>`: Sender Window Size in bytes (fixed value for Part 1)

### Client
```bash
python3 p1_client.py <SERVER_IP> <SERVER_PORT>
```
- `<SERVER_IP>`: Server's IP address to connect to
- `<SERVER_PORT>`: Server's port to connect to

### Running Experiments
```bash
sudo python3 p1_exp.py <expname>
```
- `<expname>`: Either "loss" or "jitter"
- Requires Mininet and a Ryu controller running on 127.0.0.1:6653
- Outputs CSV files: `reliability_loss.csv` or `reliability_jitter.csv`

## Architecture

### Protocol Design

The implementation is a sliding window protocol built on UDP with the following components:

**Packet Format (1200 bytes max UDP payload):**
- First 4 bytes: Sequence number (for data) or ACK number (for acknowledgments)
- Next 16 bytes: Reserved for optional features (SACK, timestamps, etc.)
- Remaining up to 1180 bytes: Actual data payload

**Reliability Mechanisms:**
- Sliding window protocol for flow control
- Cumulative ACKs (client → server indicating next expected sequence number)
- Optional: Selective ACKs (SACK) for better performance
- Timeout-based retransmission with RTO estimation
- Fast retransmit on receiving 3 duplicate ACKs

**Connection Flow:**
1. Client sends 1-byte file request (with up to 5 retries, 2-second timeout)
2. Server sends file data in packets with sequence numbers
3. Client sends ACKs back to server
4. Server sends special "EOF" segment to signal completion
5. Both client and server terminate

### Key Components

**p1_server.py**: Server implementation (currently empty/stub)
- Must bind to specified IP and port
- Read `data.txt` and send it over UDP with reliability
- Implement sliding window with fixed SWS
- Handle timeouts and retransmissions
- Send EOF marker when complete

**p1_client.py**: Client implementation (currently empty/stub)
- Request file from server with retry logic
- Receive packets and send ACKs
- Reorder packets using sequence numbers
- Write received data to `received_data.txt`
- Detect EOF and terminate

**p1_exp.py**: Mininet experiment harness
- Sets up 2-host topology (h1=server, h2=client) via switch s1
- Injects packet loss and delay/jitter using TC qdisc
- Runs experiments multiple times and collects timing/MD5 data
- Loss experiment: 1-5% loss, 20ms delay, 0ms jitter
- Jitter experiment: 1% loss, 20ms delay, 20-100ms jitter

**data.txt**: File to transfer (Project Gutenberg text file)

## Important Implementation Notes

- UDP maximum payload is 1200 bytes total (20 bytes header + up to 1180 bytes data)
- Sequence numbers are 4-byte integers representing byte offsets
- ACK numbers follow TCP-style cumulative acknowledgment (next expected byte)
- Server handles one client at a time (no concurrent connections)
- The file content will NOT contain "EOF" string naturally - use it as termination marker
- Client initial request can be any 1-byte message
- For Part 1: No congestion control, just fixed SWS parameter

## Performance Considerations

Grading includes:
- 50% correctness and completion
- 25% meeting performance targets
- 25% relative performance ranking against other submissions

Key optimizations to consider:
- Efficient RTO estimation (similar to TCP's exponential weighted moving average)
- Fast retransmit reduces wait time vs timeout-only approach
- SACK can significantly improve performance in lossy conditions
- Proper window management to maximize throughput without overwhelming network

## Testing

Experiments use Mininet to simulate network conditions:
- Requires Ryu controller running as learning switch
- Tests with varying loss rates (1-5%) and delay jitter (20-100ms)
- Each experiment runs 5 iterations for statistical validity
- Verify correctness by comparing MD5 hash of `received_data.txt` against `data.txt`
