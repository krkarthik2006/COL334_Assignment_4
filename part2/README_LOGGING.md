# TCP CUBIC Logging and Analysis Tools

This directory contains tools for debugging and analyzing TCP CUBIC congestion control implementation.

## Overview

The server automatically logs detailed congestion control metrics during file transfer:
- Congestion window (cwnd) evolution over time
- Slow start threshold (ssthresh) changes
- Mode transitions (slow start ↔ congestion avoidance)
- Congestion events (timeouts, fast retransmits)
- Sending rate and throughput
- RTO (Retransmission Timeout) evolution
- CUBIC-specific variables (W_max, K, epoch timing)

## Files

- **p2_server.py** - Server with built-in logging
- **p2_client.py** - Client implementation
- **cubic_logs.csv** - Generated log file (after running server)
- **plot_cubic.py** - Visualization script
- **analyze_cubic.py** - Quick text-based analysis

## Usage

### 1. Run Server and Client

```bash
# Terminal 1: Start server
python3 p2_server.py <SERVER_IP> <SERVER_PORT>

# Terminal 2: Start client
python3 p2_client.py <SERVER_IP> <SERVER_PORT> <PREFIX>
```

The server will automatically generate `cubic_logs.csv` after the transfer completes.

### 2. Quick Text Analysis

For a quick summary without plots:

```bash
python3 analyze_cubic.py cubic_logs.csv
```

This prints:
- Transfer summary (duration, bytes sent/acked)
- Throughput and link utilization
- Congestion window statistics
- Event timeline (congestion events, fast retransmits, timeouts)
- Mode distribution (slow start vs congestion avoidance)
- CUBIC-specific metrics

### 3. Visual Analysis

To create comprehensive plots:

```bash
python3 plot_cubic.py cubic_logs.csv
```

This generates:
1. **Congestion Window Evolution** - Shows cwnd and ssthresh over time, with congestion events marked
2. **Mode Transitions** - Visualizes slow start vs congestion avoidance phases
3. **Sending Rate vs Throughput** - Compares sending rate with actual throughput
4. **RTO Evolution** - Shows how retransmission timeout adapts
5. **W_max Tracking** - CUBIC's memory of last maximum window
6. **Cumulative Data Transfer** - Bytes sent vs bytes acknowledged
7. **Event Timeline** - All significant events in chronological order

The plot is saved as `cubic_logs_analysis.png`.

## Logged Metrics

Each log entry contains:

| Metric | Description |
|--------|-------------|
| `time` | Time since transfer start (seconds) |
| `cwnd` | Congestion window size (bytes) |
| `ssthresh` | Slow start threshold (bytes) |
| `mode` | Current mode (slow_start or congestion_avoidance) |
| `event` | Event type (see below) |
| `rto` | Retransmission timeout (seconds) |
| `W_max` | CUBIC's W_max parameter (MSS units) |
| `sending_rate` | Instantaneous sending rate (bytes/sec) |
| `throughput` | Average throughput (bytes/sec) |
| `bytes_sent` | Cumulative bytes sent |
| `bytes_acked` | Cumulative bytes acknowledged |
| `extra` | Additional event-specific information |

## Event Types

- **transfer_start** - Beginning of file transfer
- **slow_start_growth** - cwnd increased during slow start
- **epoch_start** - New CUBIC epoch started (entered congestion avoidance)
- **congestion** - Generic congestion event (reduced cwnd)
- **fast_retransmit** - 3 duplicate ACKs received
- **timeout** - Packet timeout occurred
- **periodic** - Regular status snapshot (every 0.1s)
- **transfer_complete** - End of file transfer

## Interpreting Results

### Good Behavior
- cwnd grows smoothly (exponential in slow start, cubic in CA)
- ssthresh stabilizes after initial congestion
- Few congestion events (mostly fast retransmits, minimal timeouts)
- Throughput approaches sending rate (high efficiency)
- Link utilization reaches target (e.g., 50-60% for 10 Mbps link)

### Problem Indicators
- **cwnd oscillates wildly** → Overreacting to congestion
- **cwnd grows indefinitely** → Not detecting/responding to loss
- **cwnd collapses to minimum** → Too aggressive backoff
- **Many timeouts** → RTO too small or serious packet loss
- **Sending rate >> throughput** → Filling queues, wasting bandwidth
- **Low link utilization** → cwnd too conservative

## Debugging Tips

1. **If cwnd never grows:**
   - Check slow start logic (should double per RTT)
   - Verify ACKs are being received
   - Check if ssthresh is too low

2. **If cwnd collapses:**
   - Check beta value (should be 0.7 for CUBIC)
   - Verify only real congestion triggers reduction
   - Check for spurious retransmissions

3. **If low throughput:**
   - Compare sending_rate vs throughput
   - Check if cwnd is too small
   - Look for frequent congestion events

4. **If many timeouts:**
   - Check RTO calculation (should use 4*rttvar)
   - Verify RTO bounds (MIN_TIMEOUT, MAX_TIMEOUT)
   - Check if exponential backoff is working

## Example Analysis Workflow

```bash
# 1. Run transfer
python3 p2_server.py 10.0.0.1 6555 &
python3 p2_client.py 10.0.0.1 6555 1

# 2. Quick check
python3 analyze_cubic.py cubic_logs.csv

# 3. Look for issues in output:
#    - Link utilization too low?
#    - Too many timeouts?
#    - cwnd not growing?

# 4. Visual inspection
python3 plot_cubic.py cubic_logs.csv

# 5. Identify problems in plots:
#    - cwnd growth curve shape
#    - Congestion event frequency
#    - Sending rate vs throughput gap

# 6. Adjust parameters in p2_server.py:
#    - Initial cwnd
#    - C, beta parameters
#    - RTO bounds
#    - ssthresh

# 7. Repeat
```

## Requirements

```bash
pip install pandas matplotlib
```

## Notes

- Logs are overwritten each run
- Periodic logging happens every 0.1 seconds
- Large transfers may generate large CSV files
- Plot generation requires display (use analyze_cubic.py on headless systems)
