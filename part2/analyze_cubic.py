#!/usr/bin/env python3
"""
Quick analysis of TCP CUBIC logs without plotting.
Usage: python3 analyze_cubic.py [cubic_logs.csv]
"""

import sys
import pandas as pd

def analyze_cubic_logs(log_file='cubic_logs.csv'):
    """
    Analyze TCP CUBIC logs and print key metrics.
    """
    try:
        df = pd.read_csv(log_file)
    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading log file: {e}")
        return

    print(f"\n{'='*60}")
    print(f"TCP CUBIC Log Analysis: {log_file}")
    print(f"{'='*60}\n")

    # Basic statistics
    print("=== Transfer Summary ===")
    print(f"Total duration: {df['time'].max():.2f} seconds")
    print(f"Total log entries: {len(df)}")
    print(f"Bytes sent: {df['bytes_sent'].iloc[-1]:,} bytes ({df['bytes_sent'].iloc[-1]/1024/1024:.2f} MB)")
    print(f"Bytes acked: {df['bytes_acked'].iloc[-1]:,} bytes ({df['bytes_acked'].iloc[-1]/1024/1024:.2f} MB)")

    # Throughput
    print("\n=== Throughput ===")
    avg_throughput_mbps = df['throughput'].mean() * 8 / 1e6
    final_throughput_mbps = df['throughput'].iloc[-1] * 8 / 1e6
    max_throughput_mbps = df['throughput'].max() * 8 / 1e6
    print(f"Average throughput: {avg_throughput_mbps:.2f} Mbps")
    print(f"Final throughput: {final_throughput_mbps:.2f} Mbps")
    print(f"Max throughput: {max_throughput_mbps:.2f} Mbps")

    # Link utilization (assuming 10 Mbps link capacity)
    link_capacity_mbps = 10.0
    avg_utilization = (avg_throughput_mbps / link_capacity_mbps) * 100
    final_utilization = (final_throughput_mbps / link_capacity_mbps) * 100
    print(f"Average link utilization: {avg_utilization:.2f}%")
    print(f"Final link utilization: {final_utilization:.2f}%")

    # Congestion window
    print("\n=== Congestion Window (cwnd) ===")
    print(f"Initial cwnd: {df['cwnd'].iloc[0]:.0f} bytes ({df['cwnd'].iloc[0]/1180:.2f} MSS)")
    print(f"Final cwnd: {df['cwnd'].iloc[-1]:.0f} bytes ({df['cwnd'].iloc[-1]/1180:.2f} MSS)")
    print(f"Max cwnd: {df['cwnd'].max():.0f} bytes ({df['cwnd'].max()/1180:.2f} MSS)")
    print(f"Min cwnd: {df['cwnd'].min():.0f} bytes ({df['cwnd'].min()/1180:.2f} MSS)")

    # ssthresh
    print("\n=== Slow Start Threshold (ssthresh) ===")
    print(f"Initial ssthresh: {df['ssthresh'].iloc[0]:.0f} bytes ({df['ssthresh'].iloc[0]/1180:.2f} MSS)")
    print(f"Final ssthresh: {df['ssthresh'].iloc[-1]:.0f} bytes ({df['ssthresh'].iloc[-1]/1180:.2f} MSS)")
    print(f"Min ssthresh: {df['ssthresh'].min():.0f} bytes ({df['ssthresh'].min()/1180:.2f} MSS)")

    # Events
    print("\n=== Events ===")
    event_counts = df['event'].value_counts()
    for event, count in event_counts.items():
        if event != 'periodic':
            print(f"{event}: {count}")

    # Congestion events details
    congestion_events = df[df['event'].isin(['congestion', 'fast_retransmit', 'timeout'])]
    if len(congestion_events) > 0:
        print(f"\nTotal congestion events: {len(congestion_events)}")
        print(f"  - Fast retransmits: {len(df[df['event'] == 'fast_retransmit'])}")
        print(f"  - Timeouts: {len(df[df['event'] == 'timeout'])}")

        print("\nCongestion event times:")
        for _, event in congestion_events.iterrows():
            print(f"  {event['time']:.2f}s: {event['event']} - {event['extra']}")

    # Mode distribution
    print("\n=== Mode Distribution ===")
    mode_counts = df['mode'].value_counts()
    for mode, count in mode_counts.items():
        percentage = (count / len(df)) * 100
        print(f"{mode}: {count} entries ({percentage:.1f}%)")

    # RTO statistics
    print("\n=== RTO (Retransmission Timeout) ===")
    print(f"Average RTO: {df['rto'].mean():.3f} seconds")
    print(f"Min RTO: {df['rto'].min():.3f} seconds")
    print(f"Max RTO: {df['rto'].max():.3f} seconds")
    print(f"Final RTO: {df['rto'].iloc[-1]:.3f} seconds")

    # CUBIC-specific
    print("\n=== CUBIC Specific ===")
    print(f"Final W_max: {df['W_max'].iloc[-1]:.2f} MSS")
    print(f"Max W_max: {df['W_max'].max():.2f} MSS")

    # Sending rate vs throughput comparison
    print("\n=== Sending Rate vs Throughput ===")
    avg_sending_rate_mbps = df['sending_rate'].mean() * 8 / 1e6
    print(f"Average sending rate: {avg_sending_rate_mbps:.2f} Mbps")
    print(f"Average throughput: {avg_throughput_mbps:.2f} Mbps")
    efficiency = (avg_throughput_mbps / avg_sending_rate_mbps * 100) if avg_sending_rate_mbps > 0 else 0
    print(f"Efficiency (acked/sent): {efficiency:.1f}%")

    # Growth analysis
    print("\n=== Growth Analysis ===")
    slow_start_data = df[df['mode'] == 'slow_start']
    ca_data = df[df['mode'] == 'congestion_avoidance']

    if len(slow_start_data) > 1:
        ss_cwnd_growth = slow_start_data['cwnd'].iloc[-1] - slow_start_data['cwnd'].iloc[0]
        ss_duration = slow_start_data['time'].iloc[-1] - slow_start_data['time'].iloc[0]
        print(f"Slow start duration: {ss_duration:.2f}s")
        print(f"Slow start cwnd growth: {ss_cwnd_growth:.0f} bytes ({ss_cwnd_growth/1180:.2f} MSS)")

    if len(ca_data) > 1:
        ca_duration = ca_data['time'].iloc[-1] - ca_data['time'].iloc[0]
        print(f"Congestion avoidance duration: {ca_duration:.2f}s")

    print(f"\n{'='*60}\n")

if __name__ == '__main__':
    log_file = sys.argv[1] if len(sys.argv) > 1 else 'cubic_logs.csv'
    analyze_cubic_logs(log_file)
