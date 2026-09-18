import matplotlib.pyplot as plt

# Benchmark data
pbft_runs = [1, 2, 3, 4, 5]
pbft_avg_throughputs = [
    707.047619047619,
    858.6666666666666,
    1144.888888888889,
    1144.888888888889,
    1064.888888888889,
]
pbft_avg_latencies = [
    0.000631235,
    0.0006088765,
    0.000637675,
    0.000566652,
    0.000548829,
]

poe_runs = [1, 2, 3, 4, 5]
poe_avg_throughputs = [
    1144.888888888889,
    1144.888888888889,
    624.4848484848485,
    1091.5555555555557,
    606.1176470588235,
]
poe_avg_latencies = [
    0.000656722,
    0.00053795,
    0.000635217,
    0.000560667,
    0.00043056699999999997,
]

# Determine the full range of runs (so ticks go from 1 up to the maximum run number)
all_runs = sorted(set(pbft_runs + poe_runs))
min_run, max_run = all_runs[0], all_runs[-1]
ticks = list(range(min_run, max_run + 1))

# Create a figure with two subplots: one for throughput, one for latency
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

# Plot average throughput
ax1.plot(pbft_runs, pbft_avg_throughputs, marker='o', label='PBFT')
ax1.plot(poe_runs, poe_avg_throughputs, marker='s', label='PoE')
ax1.set_title('Average Throughput over Runs')
ax1.set_xlabel('Run Number')
ax1.set_ylabel('Throughput')
ax1.set_xticks(ticks)               # force integer ticks
ax1.legend()
ax1.grid(True)

# Plot average latency
ax2.plot(pbft_runs, pbft_avg_latencies, marker='o', label='PBFT')
ax2.plot(poe_runs, poe_avg_latencies, marker='s', label='PoE')
ax2.set_title('Average Latency over Runs')
ax2.set_xlabel('Run Number')
ax2.set_ylabel('Latency (seconds)')
ax2.set_xticks(ticks)               # force integer ticks
ax2.legend()
ax2.grid(True)

plt.tight_layout()

# Save the figure to a file
plt.savefig('benchmark_results.png', dpi=300)

# Optionally display it on screen
plt.show()
