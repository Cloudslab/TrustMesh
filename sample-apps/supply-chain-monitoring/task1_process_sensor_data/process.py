import json
import time
import numpy as np
import asyncio
import logging
import traceback
from datetime import datetime
from typing import List, Dict
from scipy import signal, stats
from scipy.fftpack import fft, ifft
from sklearn.preprocessing import StandardScaler
import pywt
from itertools import combinations
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SignalProcessor:
    def __init__(self):
        self.sampling_rate = 1  # 1 Hz
        self.nyquist = self.sampling_rate / 2

    def compute_intensive_features(self, data: np.ndarray, n_iterations: int = 1000) -> Dict:
        """Compute computationally intensive features"""
        features = {}

        # Wavelet analysis with multiple wavelets and decomposition levels
        wavelet_families = ['db4', 'sym4', 'coif4', 'bior4.4']
        for wavelet in wavelet_families:
            # Intensive wavelet computations
            coeffs_list = []
            for level in range(1, min(4, len(data) // 2)):
                # Decompose signal
                coeffs = pywt.wavedec(data, wavelet, level=level)
                coeffs_list.append(coeffs)

                # Apply different thresholding strategies
                thresholds = [0.1, 0.3, 0.5, 0.7, 0.9]
                for threshold in thresholds:
                    # Copy coefficients for this iteration
                    coeffs_modified = [c.copy() for c in coeffs]

                    # Apply threshold
                    for i in range(1, len(coeffs_modified)):
                        coeffs_modified[i] = pywt.threshold(
                            coeffs_modified[i],
                            threshold * np.max(np.abs(coeffs_modified[i])),
                            mode='soft'
                        )

                    # Reconstruct and compute features
                    reconstructed = pywt.waverec(coeffs_modified, wavelet)
                    if len(reconstructed) > 0:  # Ensure we have valid reconstruction
                        features[f'{wavelet}_level_{level}_thresh_{threshold}'] = {
                            'mean': float(np.mean(reconstructed)),
                            'std': float(np.std(reconstructed)),
                            'energy': float(np.sum(reconstructed ** 2)),
                            'entropy': float(-np.sum(
                                reconstructed ** 2 * np.log(reconstructed ** 2 + 1e-10)
                            ))
                        }

        # Intensive frequency domain analysis
        # Compute FFT
        fft_vals = np.fft.fft(data)
        freqs = np.fft.fftfreq(len(data), d=1 / self.sampling_rate)

        # Analyze different frequency bands
        for i in range(n_iterations):
            # Randomly select frequency band
            band_start = np.random.uniform(0, self.nyquist / 2)
            band_end = band_start + np.random.uniform(0, self.nyquist / 2)

            # Filter frequencies in band
            mask = (np.abs(freqs) >= band_start) & (np.abs(freqs) <= band_end)
            band_fft = fft_vals.copy()
            band_fft[~mask] = 0

            # Inverse FFT for this band
            band_signal = np.real(np.fft.ifft(band_fft))

            # Compute features for this band
            features[f'freq_band_{i}'] = {
                'band_range': (float(band_start), float(band_end)),
                'power': float(np.sum(np.abs(band_signal) ** 2)),
                'mean': float(np.mean(band_signal)),
                'std': float(np.std(band_signal))
            }

        # Time-domain intensive analysis
        for i in range(n_iterations):
            # Generate random segment indices ensuring they're within bounds
            if len(data) >= 3:  # Ensure we have enough data points
                start_idx = np.random.randint(0, len(data) - 2)
                end_idx = start_idx + min(3, len(data) - start_idx)
                segment = data[start_idx:end_idx]

                # Compute various statistical measures
                features[f'segment_{i}'] = {
                    'mean': float(np.mean(segment)),
                    'std': float(np.std(segment)),
                    'skew': float(stats.skew(segment)) if len(segment) >= 3 else 0.0,
                    'kurtosis': float(stats.kurtosis(segment)) if len(segment) >= 4 else 0.0,
                    'range': float(np.ptp(segment))
                }

        return features

    def compute_intensive_cross_correlations(self, data1: np.ndarray, data2: np.ndarray,
                                             n_iterations: int = 1000) -> Dict:
        """Compute intensive cross-correlation analysis"""
        results = {}

        # Ensure signals are the same length
        min_length = min(len(data1), len(data2))
        data1 = data1[:min_length]
        data2 = data2[:min_length]

        # Compute baseline correlation
        correlation = signal.correlate(data1, data2, mode='full')
        lags = signal.correlation_lags(len(data1), len(data2))

        results['baseline'] = {
            'max_correlation': float(np.max(np.abs(correlation))),
            'lag_at_max': int(lags[np.argmax(np.abs(correlation))]),
            'mean_correlation': float(np.mean(np.abs(correlation)))
        }

        # Perform intensive correlation analysis with different preprocessing
        for i in range(n_iterations):
            # Apply random filtering
            filter_order = np.random.randint(1, 4)
            cutoff = np.random.uniform(0.1, 0.4)

            try:
                # Design and apply filter
                b, a = signal.butter(filter_order, cutoff, btype='low')
                data1_filtered = signal.filtfilt(b, a, data1)
                data2_filtered = signal.filtfilt(b, a, data2)

                # Compute correlation
                correlation = signal.correlate(data1_filtered, data2_filtered, mode='full')
                lags = signal.correlation_lags(len(data1_filtered), len(data2_filtered))

                key = f'iteration_{i}_order_{filter_order}_cutoff_{cutoff:.2f}'
                results[key] = {
                    'max_correlation': float(np.max(np.abs(correlation))),
                    'lag_at_max': int(lags[np.argmax(np.abs(correlation))]),
                    'mean_correlation': float(np.mean(np.abs(correlation)))
                }
            except Exception as e:
                logger.warning(f"Failed correlation iteration {i}: {str(e)}")
                continue

        return results


def process_readings_batch(readings: List[Dict]) -> Dict:
    """Process a batch of readings with intensive computations"""
    try:
        # Extract values
        temperatures = np.array([float(r['temperature']) for r in readings])
        humidities = np.array([float(r['humidity']) for r in readings])
        timestamps = [r['timestamp'] for r in readings]
        device_id = readings[0]['device_id']

        # Initialize signal processor
        processor = SignalProcessor()

        # Compute intensive features for both temperature and humidity
        temp_features = processor.compute_intensive_features(temperatures)
        humid_features = processor.compute_intensive_features(humidities)

        # Compute intensive cross-correlations
        correlation_analysis = processor.compute_intensive_cross_correlations(
            temperatures, humidities
        )

        # Calculate environmental stability index using processed features
        stability_factors = []

        # Add wavelet-based stability factors
        for key, features in temp_features.items():
            if 'entropy' in features:
                stability_factors.append(1.0 / (1.0 + features['entropy']))

        for key, features in humid_features.items():
            if 'entropy' in features:
                stability_factors.append(1.0 / (1.0 + features['entropy']))

        # Add correlation-based stability factors
        for key, corr_data in correlation_analysis.items():
            stability_factors.append(abs(corr_data['max_correlation']))

        # Calculate final stability index
        stability_index = np.mean(stability_factors) * 100 if stability_factors else 50.0

        return {
            "device_id": device_id,
            "start_time": min(timestamps),
            "end_time": max(timestamps),
            "readings_count": len(readings),
            "environmental_stability_index": float(stability_index),
            "temperature_analysis": temp_features,
            "humidity_analysis": humid_features,
            "correlation_analysis": correlation_analysis,
            "processed_timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        raise


async def read_json(reader):
    """Read JSON data from the stream"""
    buffer = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            raise ValueError("Connection closed before receiving a valid JSON object.")
        buffer += chunk
        try:
            return json.loads(buffer.decode())
        except json.JSONDecodeError:
            continue


async def handle_client(reader, writer):
    try:
        start_time = time.perf_counter()
        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        # Process the batch with intensive computations
        processed_data = process_readings_batch(input_data['data'])

        output = {
            "data": [processed_data],
            "total_task_time": input_data['total_task_time'] + time.perf_counter() - start_time
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info("Processed batch successfully")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error(traceback.format_exc())
        error_msg = json.dumps({"error": f"An internal error occurred: {str(e)}"})
        writer.write(error_msg.encode())
    finally:
        await writer.drain()
        writer.close()
        logger.info("Connection closed")


async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 12345)
    addr = server.sockets[0].getsockname()
    logger.info(f'Serving on {addr}')
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
