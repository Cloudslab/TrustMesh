import argparse
import logging
import time
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict

from transaction_initiator.transaction_initiator import transaction_creator
from response_manager.response_manager import IoTDeviceManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SAMPLE_INTERVAL = 1  # 1 reading per second
WINDOW_SIZE = 10  # 10 seconds per window
SEND_INTERVAL = 10  # Send data every 5 seconds


class ColdChainSimulator:
    def __init__(self, device_id: str):
        # Normal operating parameters
        self.ideal_temp = 5.0
        self.ideal_humidity = 60.0
        self.temp_std = 0.5
        self.humidity_std = 2.0
        self.device_id = device_id

        # Drift and anomaly parameters
        self.temp_drift = 0.0
        self.humidity_drift = 0.0
        self.current_time = datetime.now()

        # Anomaly generation
        self.in_anomaly = False
        self.anomaly_countdown = 0
        self.anomaly_probability = 0.05  # 5% chance of starting an anomaly

    def _maybe_start_anomaly(self):
        """Possibly start an anomaly condition"""
        if not self.in_anomaly and np.random.random() < self.anomaly_probability:
            self.in_anomaly = True
            self.anomaly_countdown = np.random.randint(20, 50)  # Anomaly lasts 20-50 seconds

            # Choose anomaly type
            anomaly_type = np.random.choice(['temp_spike', 'humidity_spike', 'both_spike', 'gradual_drift'])

            if anomaly_type == 'temp_spike':
                self.temp_drift = np.random.uniform(3, 5)
            elif anomaly_type == 'humidity_spike':
                self.humidity_drift = np.random.uniform(10, 15)
            elif anomaly_type == 'both_spike':
                self.temp_drift = np.random.uniform(3, 5)
                self.humidity_drift = np.random.uniform(10, 15)
            else:  # gradual_drift
                self.temp_drift = np.random.uniform(2, 3)
                self.humidity_drift = np.random.uniform(7, 10)

    def _update_anomaly(self):
        """Update anomaly status"""
        if self.in_anomaly:
            self.anomaly_countdown -= 1
            if self.anomaly_countdown <= 0:
                self.in_anomaly = False
                self.temp_drift = 0.0
                self.humidity_drift = 0.0

    def generate_window_readings(self) -> List[Dict]:
        """Generate 10 readings, one per second"""
        readings = []

        for _ in range(WINDOW_SIZE):
            self._maybe_start_anomaly()
            self._update_anomaly()

            # Generate base values
            temperature = np.random.normal(self.ideal_temp + self.temp_drift, self.temp_std)
            humidity = np.random.normal(self.ideal_humidity + self.humidity_drift, self.humidity_std)

            reading = {
                "temperature": round(temperature, 2),
                "humidity": round(humidity, 2),
                "timestamp": self.current_time.isoformat(),
                "device_id": self.device_id
            }

            readings.append(reading)
            self.current_time += timedelta(seconds=1)

        return readings


def continuous_cold_chain_simulation(workflow_id: str):
    logger.info(f"Starting continuous cold chain simulation for workflow ID: {workflow_id}")
    simulator = ColdChainSimulator(device_id=f"cold_chain_{workflow_id}")

    while True:
        try:
            # Generate 10 readings (one per second)
            readings = simulator.generate_window_readings()

            # Log some stats for monitoring
            temps = [r['temperature'] for r in readings]
            humids = [r['humidity'] for r in readings]
            logger.debug(f"Window stats: Temp range [{min(temps):.1f}, {max(temps):.1f}]°C, "
                         f"Humidity range [{min(humids):.1f}, {max(humids):.1f}]%")

            # Send the raw readings directly
            schedule_id = transaction_creator.create_and_send_transactions(
                readings,  # List of individual readings
                workflow_id,
                iot_device_manager.port,
                iot_device_manager.public_key
            )

            logger.info(f"Data sent to blockchain. Schedule ID: {schedule_id}")

            # Wait before next window
            time.sleep(SEND_INTERVAL)

        except Exception as ex:
            logger.error(f"Error in cold chain simulation: {str(ex)}")
            iot_device_manager.stop()
            time.sleep(5)  # Wait before retrying


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Continuously generate cold chain sensor data and send to Sawtooth blockchain.')
    parser.add_argument('workflow_id', help='Workflow ID for the cold chain monitoring process')
    args = parser.parse_args()

    # Initialize IoTDeviceManager
    iot_device_manager = IoTDeviceManager("cold-chain-sim", 5555)
    iot_device_manager.start()

    continuous_cold_chain_simulation(args.workflow_id)
