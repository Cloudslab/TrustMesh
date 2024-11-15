import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_prepare_data(csv_path):
    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    return df

def analyze_compute_nodes_by_os(df):
    # Filter compute nodes
    compute_nodes = df[df['node_name'].str.contains('compute-node')]

    # Group by OS and calculate mean resource usage
    os_stats = compute_nodes.groupby('os_image').agg({
        'cpu_usage_percent': 'mean',
        'memory_usage_percent': 'mean'
    }).round(2)

    return os_stats

def analyze_client_console(df):
    # Filter client console data
    client_console = df[df['node_name'] == 'client-console']

    # Calculate mean resource usage
    console_stats = {
        'cpu_usage': client_console['cpu_usage_percent'].mean(),
        'memory_usage': client_console['memory_usage_percent'].mean()
    }

    return console_stats

def analyze_iot_nodes_by_os(df):
    # Filter IoT nodes
    iot_nodes = df[df['node_name'].str.contains('iot-node')]

    # Group by OS and calculate mean resource usage
    iot_os_stats = iot_nodes.groupby('os_image').agg({
        'cpu_usage_percent': 'mean',
        'memory_usage_percent': 'mean'
    }).round(2)

    return iot_os_stats

def plot_resource_usage(compute_stats, iot_stats, console_stats):
    # Set up the style
    plt.style.use('seaborn')

    # Create figure with subplots
    fig = plt.figure(figsize=(15, 10))

    # compute nodes plot
    plt.subplot(2, 1, 1)
    x = range(len(compute_stats.index))
    width = 0.35

    plt.bar(x, compute_stats['cpu_usage_percent'], width, label='CPU Usage %', color='skyblue')
    plt.bar([i + width for i in x], compute_stats['memory_usage_percent'], width, label='Memory Usage %', color='lightcoral')

    plt.xlabel('Operating System')
    plt.ylabel('Usage Percentage')
    plt.title('compute Nodes Resource Usage by OS')
    plt.xticks([i + width/2 for i in x], compute_stats.index, rotation=45, ha='right')
    plt.legend()

    # IoT nodes plot
    plt.subplot(2, 1, 2)
    x_iot = range(len(iot_stats.index))

    plt.bar(x_iot, iot_stats['cpu_usage_percent'], width, label='CPU Usage %', color='skyblue')
    plt.bar([i + width for i in x_iot], iot_stats['memory_usage_percent'], width, label='Memory Usage %', color='lightcoral')

    plt.xlabel('Operating System')
    plt.ylabel('Usage Percentage')
    plt.title('IoT Nodes Resource Usage by OS')
    plt.xticks([i + width/2 for i in x_iot], iot_stats.index, rotation=45, ha='right')
    plt.legend()

    # Add client console stats as text
    fig.text(0.02, 0.98, f'Client Console Averages:\nCPU Usage: {console_stats["cpu_usage"]:.2f}%\nMemory Usage: {console_stats["memory_usage"]:.2f}%',
             fontsize=10, bbox=dict(facecolor='white', alpha=0.5))

    plt.tight_layout()
    plt.show()

def main():
    # Load and prepare data
    df = load_and_prepare_data('monitoring/node_metrics_20241108_014938.csv')

    # Analyze data
    compute_stats = analyze_compute_nodes_by_os(df)
    console_stats = analyze_client_console(df)
    iot_stats = analyze_iot_nodes_by_os(df)

    # Print statistics
    print("\ncompute Nodes Resource Usage by OS:")
    print(compute_stats)
    print("\nClient Console Average Resource Usage:")
    print(f"CPU: {console_stats['cpu_usage']:.2f}%")
    print(f"Memory: {console_stats['memory_usage']:.2f}%")
    print("\nIoT Nodes Resource Usage by OS:")
    print(iot_stats)

    # Plot the results
    plot_resource_usage(compute_stats, iot_stats, console_stats)


if __name__ == "__main__":
    main()
