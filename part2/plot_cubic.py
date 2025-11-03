#!/usr/bin/env python3
"""
Plot TCP CUBIC congestion control metrics from logged data.
Usage: python3 plot_cubic.py [cubic_logs.csv]
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

def plot_cubic_analysis(log_file='cubic_logs.csv'):
    """
    Create comprehensive plots for TCP CUBIC analysis.
    """
    # Read the CSV log file
    try:
        df = pd.read_csv(log_file)
    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")
        print("Run the server first to generate logs.")
        return
    except Exception as e:
        print(f"Error reading log file: {e}")
        return

    print(f"Loaded {len(df)} log entries from {log_file}")

    # Create figure with subplots
    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(4, 2, figure=fig, hspace=0.3, wspace=0.3)

    # 1. Congestion Window Evolution
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(df['time'], df['cwnd']/1180, 'b-', linewidth=1.5, label='cwnd')
    ax1.plot(df['time'], df['ssthresh']/1180, 'r--', linewidth=1.5, label='ssthresh')

    # Mark congestion events
    congestion_events = df[df['event'].isin(['congestion', 'fast_retransmit', 'timeout'])]
    for _, event in congestion_events.iterrows():
        color = 'red' if event['event'] == 'timeout' else 'orange'
        ax1.axvline(x=event['time'], color=color, alpha=0.3, linestyle='--', linewidth=1)

    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Window Size (MSS)', fontsize=12)
    ax1.set_title('Congestion Window Evolution', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)

    # 2. Mode Transitions
    ax2 = fig.add_subplot(gs[1, 0])
    mode_map = {'slow_start': 1, 'congestion_avoidance': 2}
    df['mode_numeric'] = df['mode'].map(mode_map)
    ax2.plot(df['time'], df['mode_numeric'], 'g-', linewidth=2)
    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_ylabel('Mode', fontsize=12)
    ax2.set_yticks([1, 2])
    ax2.set_yticklabels(['Slow Start', 'Cong. Avoid.'])
    ax2.set_title('TCP Mode Transitions', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)

    # 3. Sending Rate vs Throughput
    ax3 = fig.add_subplot(gs[1, 1])
    # Convert to Mbps
    df['sending_rate_mbps'] = df['sending_rate'] * 8 / 1e6
    df['throughput_mbps'] = df['throughput'] * 8 / 1e6

    ax3.plot(df['time'], df['sending_rate_mbps'], 'b-', linewidth=1.5, label='Sending Rate', alpha=0.7)
    ax3.plot(df['time'], df['throughput_mbps'], 'g-', linewidth=1.5, label='Throughput (Acked)', alpha=0.7)
    ax3.set_xlabel('Time (seconds)', fontsize=12)
    ax3.set_ylabel('Rate (Mbps)', fontsize=12)
    ax3.set_title('Sending Rate vs Throughput', fontsize=14, fontweight='bold')
    ax3.legend(fontsize=10)
    ax3.grid(True, alpha=0.3)

    # 4. RTO Evolution
    ax4 = fig.add_subplot(gs[2, 0])
    ax4.plot(df['time'], df['rto'], 'purple', linewidth=1.5)
    ax4.set_xlabel('Time (seconds)', fontsize=12)
    ax4.set_ylabel('RTO (seconds)', fontsize=12)
    ax4.set_title('Retransmission Timeout Evolution', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)

    # 5. W_max tracking
    ax5 = fig.add_subplot(gs[2, 1])
    ax5.plot(df['time'], df['W_max'], 'brown', linewidth=1.5)
    ax5.set_xlabel('Time (seconds)', fontsize=12)
    ax5.set_ylabel('W_max (MSS)', fontsize=12)
    ax5.set_title('CUBIC W_max (Window at Last Congestion)', fontsize=14, fontweight='bold')
    ax5.grid(True, alpha=0.3)

    # 6. Bytes Sent vs Acked
    ax6 = fig.add_subplot(gs[3, 0])
    ax6.plot(df['time'], df['bytes_sent']/1024, 'b-', linewidth=1.5, label='Bytes Sent', alpha=0.7)
    ax6.plot(df['time'], df['bytes_acked']/1024, 'g-', linewidth=1.5, label='Bytes Acked', alpha=0.7)
    ax6.set_xlabel('Time (seconds)', fontsize=12)
    ax6.set_ylabel('Data (KB)', fontsize=12)
    ax6.set_title('Cumulative Data Transfer', fontsize=14, fontweight='bold')
    ax6.legend(fontsize=10)
    ax6.grid(True, alpha=0.3)

    # 7. Event Timeline
    ax7 = fig.add_subplot(gs[3, 1])
    event_types = df['event'].unique()
    event_colors = {
        'transfer_start': 'green',
        'slow_start_growth': 'blue',
        'epoch_start': 'cyan',
        'congestion': 'red',
        'fast_retransmit': 'orange',
        'timeout': 'darkred',
        'transfer_complete': 'black',
        'periodic': 'lightgray'
    }

    for i, event_type in enumerate(event_types):
        if event_type == 'periodic':
            continue  # Skip periodic events for clarity
        event_data = df[df['event'] == event_type]
        ax7.scatter(event_data['time'], [i]*len(event_data),
                   color=event_colors.get(event_type, 'gray'),
                   label=event_type, alpha=0.7, s=50)

    ax7.set_xlabel('Time (seconds)', fontsize=12)
    ax7.set_ylabel('Event Type', fontsize=12)
    ax7.set_title('Event Timeline', fontsize=14, fontweight='bold')
    ax7.legend(fontsize=8, loc='upper left', ncol=2)
    ax7.grid(True, alpha=0.3, axis='x')

    # Overall title
    fig.suptitle('TCP CUBIC Congestion Control Analysis',
                 fontsize=16, fontweight='bold', y=0.995)

    # Save figure
    output_file = log_file.replace('.csv', '_analysis.png')
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")

    # Print statistics
    print("\n=== Analysis Summary ===")
    print(f"Transfer duration: {df['time'].max():.2f} seconds")
    print(f"Final cwnd: {df['cwnd'].iloc[-1]:.0f} bytes ({df['cwnd'].iloc[-1]/1180:.2f} MSS)")
    print(f"Max cwnd: {df['cwnd'].max():.0f} bytes ({df['cwnd'].max()/1180:.2f} MSS)")
    print(f"Final throughput: {df['throughput_mbps'].iloc[-1]:.2f} Mbps")
    print(f"Average throughput: {df['throughput_mbps'].mean():.2f} Mbps")

    congestion_count = len(df[df['event'].isin(['congestion', 'fast_retransmit', 'timeout'])])
    fast_retrans = len(df[df['event'] == 'fast_retransmit'])
    timeouts = len(df[df['event'] == 'timeout'])
    print(f"\nCongestion events: {congestion_count} (Fast retransmit: {fast_retrans}, Timeouts: {timeouts})")

    # Show plot
    plt.show()

if __name__ == '__main__':
    log_file = sys.argv[1] if len(sys.argv) > 1 else 'cubic_logs.csv'
    plot_cubic_analysis(log_file)
