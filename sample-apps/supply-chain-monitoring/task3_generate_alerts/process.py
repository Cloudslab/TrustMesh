# alert_generation.py
import asyncio
import json
import logging
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
from collections import defaultdict
import warnings

warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class AlertCategory(Enum):
    TEMPERATURE_VIOLATION = "TEMPERATURE_VIOLATION"
    HUMIDITY_VIOLATION = "HUMIDITY_VIOLATION"
    STABILITY_BREACH = "STABILITY_BREACH"
    PATTERN_ANOMALY = "PATTERN_ANOMALY"
    CORRELATION_DEVIATION = "CORRELATION_DEVIATION"
    SYSTEM_PERFORMANCE = "SYSTEM_PERFORMANCE"


class AlertContext:
    def __init__(self):
        self.historical_alerts = defaultdict(list)  # Device-specific alert history
        self.incident_tracking = defaultdict(dict)  # Track ongoing incidents
        self.pattern_memory = defaultdict(list)  # Store pattern data
        self.scaler = MinMaxScaler()

    def update_historical_context(self, device_id: str, alert: Dict):
        """Update historical context for a device"""
        self.historical_alerts[device_id].append({
            'timestamp': datetime.fromisoformat(alert['timestamp']),
            'severity': alert['severity'],
            'categories': alert['categories'],
            'conditions': alert['conditions']
        })

        # Keep only last 30 days of history
        cutoff = datetime.now() - timedelta(days=30)
        self.historical_alerts[device_id] = [
            alert for alert in self.historical_alerts[device_id]
            if alert['timestamp'] > cutoff
        ]

    def get_alert_frequency(self, device_id: str, timeframe_hours: int = 24) -> Dict:
        """Calculate alert frequency statistics for a device"""
        if device_id not in self.historical_alerts:
            return {'total': 0, 'by_severity': {}, 'by_category': {}}

        cutoff = datetime.now() - timedelta(hours=timeframe_hours)
        recent_alerts = [
            alert for alert in self.historical_alerts[device_id]
            if alert['timestamp'] > cutoff
        ]

        severity_counts = defaultdict(int)
        category_counts = defaultdict(int)

        for alert in recent_alerts:
            severity_counts[alert['severity']] += 1
            for category in alert['categories']:
                category_counts[category] += 1

        return {
            'total': len(recent_alerts),
            'by_severity': dict(severity_counts),
            'by_category': dict(category_counts)
        }

    def analyze_patterns(self, device_id: str, current_data: Dict) -> Dict:
        """Analyze patterns in device behavior"""
        self.pattern_memory[device_id].append({
            'timestamp': datetime.now(),
            'stability_index': current_data['environmental_stability'],
            'temperature_stats': current_data['temperature_analysis']['features'],
            'humidity_stats': current_data['humidity_analysis']['features']
        })

        # Keep last 7 days of pattern data
        cutoff = datetime.now() - timedelta(days=7)
        self.pattern_memory[device_id] = [
            data for data in self.pattern_memory[device_id]
            if data['timestamp'] > cutoff
        ]

        if len(self.pattern_memory[device_id]) < 2:
            return {}

        # Create time series for analysis
        df = pd.DataFrame(self.pattern_memory[device_id])

        # Calculate trend statistics
        stability_trend = np.polyfit(range(len(df)), df['stability_index'], deg=1)[0]

        # Calculate volatility
        stability_volatility = df['stability_index'].std()

        # Detect seasonal patterns if enough data
        seasonal_patterns = {}
        if len(df) >= 24:
            try:
                for col in ['stability_index']:
                    series = df[col]
                    fft_vals = np.fft.fft(series)
                    freqs = np.fft.fftfreq(len(series))
                    dominant_freq = freqs[np.argmax(np.abs(fft_vals[1:])) + 1]
                    seasonal_patterns[col] = {
                        'period_hours': abs(1 / dominant_freq) if dominant_freq != 0 else 0,
                        'strength': np.max(np.abs(fft_vals[1:])) / len(series)
                    }
            except Exception as e:
                logger.warning(f"Could not compute seasonal patterns: {str(e)}")

        return {
            'stability_trend': float(stability_trend),
            'stability_volatility': float(stability_volatility),
            'seasonal_patterns': seasonal_patterns,
            'data_points': len(df)
        }


class RiskAssessor:
    def __init__(self, context: AlertContext):
        self.context = context

    def calculate_base_risk_score(self, anomaly_data: Dict) -> float:
        """Calculate base risk score from anomaly detection results"""
        # Combine multiple factors with weighted importance
        weights = {
            'anomaly_score': 0.3,
            'stability_index': 0.2,
            'temp_deviation': 0.25,
            'humidity_deviation': 0.25
        }

        # Normalize environmental stability index (higher is better, so invert)
        stability_risk = 1 - (anomaly_data['environmental_stability'] / 100)

        # Get temperature and humidity deviations
        temp_stats = anomaly_data['temperature_analysis']['features']
        humid_stats = anomaly_data['humidity_analysis']['features']

        temp_deviation = abs(temp_stats['mean'] - 5.0) / 5.0  # Normalized deviation from ideal temp
        humid_deviation = abs(humid_stats['mean'] - 60.0) / 60.0  # Normalized deviation from ideal humidity

        # Combine scores
        risk_score = (
                weights['anomaly_score'] * anomaly_data['anomaly_score'] +
                weights['stability_index'] * stability_risk +
                weights['temp_deviation'] * temp_deviation +
                weights['humidity_deviation'] * humid_deviation
        )

        return min(max(risk_score, 0.0), 1.0)  # Ensure score is between 0 and 1

    def adjust_risk_for_context(self, base_risk: float, device_id: str, anomaly_data: Dict) -> float:
        """Adjust risk score based on historical context and patterns"""
        # Get alert frequency statistics
        alert_freq = self.context.get_alert_frequency(device_id)

        # Analyze patterns
        pattern_analysis = self.context.analyze_patterns(device_id, anomaly_data)

        # Risk multipliers
        multipliers = 1.0

        # Adjust for alert frequency
        if alert_freq['total'] > 10:  # High alert frequency
            multipliers *= 1.2
        elif alert_freq['total'] > 5:  # Moderate alert frequency
            multipliers *= 1.1

        # Adjust for negative trends in patterns
        if pattern_analysis.get('stability_trend', 0) < -0.1:  # Deteriorating stability
            multipliers *= 1.15

        # Adjust for high volatility
        if pattern_analysis.get('stability_volatility', 0) > 0.2:
            multipliers *= 1.1

        # Calculate final risk score
        adjusted_risk = base_risk * multipliers

        return min(max(adjusted_risk, 0.0), 1.0)  # Ensure score is between 0 and 1

    def determine_categories(self, anomaly_data: Dict, risk_score: float) -> List[str]:
        """Determine applicable alert categories based on anomaly data"""
        categories = []

        # Temperature violation check
        temp_stats = anomaly_data['temperature_analysis']['features']
        if abs(temp_stats['mean'] - 5.0) > 2.0:
            categories.append(AlertCategory.TEMPERATURE_VIOLATION.value)

        # Humidity violation check
        humid_stats = anomaly_data['humidity_analysis']['features']
        if abs(humid_stats['mean'] - 60.0) > 10.0:
            categories.append(AlertCategory.HUMIDITY_VIOLATION.value)

        # Stability breach check
        if anomaly_data['environmental_stability'] < 70.0:
            categories.append(AlertCategory.STABILITY_BREACH.value)

        # Pattern anomaly check
        if anomaly_data['anomaly_score'] > 0.8:
            categories.append(AlertCategory.PATTERN_ANOMALY.value)

        # Correlation deviation check
        if abs(anomaly_data['correlation_analysis']['correlation_strength']) < 0.3:
            categories.append(AlertCategory.CORRELATION_DEVIATION.value)

        return categories

    def determine_severity(self, risk_score: float, categories: List[str]) -> AlertSeverity:
        """Determine alert severity based on risk score and categories"""
        if risk_score > 0.8:
            return AlertSeverity.CRITICAL
        elif risk_score > 0.6:
            return AlertSeverity.HIGH
        elif risk_score > 0.4:
            return AlertSeverity.MEDIUM
        elif risk_score > 0.2:
            return AlertSeverity.LOW
        else:
            return AlertSeverity.INFO


def generate_detailed_message(anomaly_data: Dict, risk_score: float,
                              severity: AlertSeverity, categories: List[str],
                              pattern_analysis: Dict) -> str:
    """Generate comprehensive alert message with detailed analysis"""
    message_parts = []

    # Header
    message_parts.append(f"Cold Chain Alert - {severity.value}")
    message_parts.append(f"Device: {anomaly_data['device_id']}")
    message_parts.append(f"Time Window: {anomaly_data['window_start']} to {anomaly_data['window_end']}")
    message_parts.append(f"Risk Score: {risk_score:.2f}")

    # Categories
    message_parts.append("\nAlert Categories:")
    for category in categories:
        message_parts.append(f"- {category}")

    # Environmental Conditions
    message_parts.append("\nEnvironmental Conditions:")
    temp_stats = anomaly_data['temperature_analysis']['features']
    humid_stats = anomaly_data['humidity_analysis']['features']

    message_parts.append(f"Temperature Statistics:")
    message_parts.append(f"- Mean: {temp_stats['mean']:.1f}°C")
    message_parts.append(f"- Standard Deviation: {temp_stats['std']:.2f}°C")
    message_parts.append(f"- Trend Strength: {temp_stats['trend_strength']:.2f}")

    message_parts.append(f"\nHumidity Statistics:")
    message_parts.append(f"- Mean: {humid_stats['mean']:.1f}%")
    message_parts.append(f"- Standard Deviation: {humid_stats['std']:.2f}%")
    message_parts.append(f"- Trend Strength: {humid_stats['trend_strength']:.2f}")

    # Stability Analysis
    message_parts.append(f"\nStability Analysis:")
    message_parts.append(f"- Environmental Stability Index: {anomaly_data['environmental_stability']:.1f}%")
    message_parts.append(f"- Correlation Strength: {anomaly_data['correlation_analysis']['correlation_strength']:.2f}")
    message_parts.append(f"- Anomaly Score: {anomaly_data['anomaly_score']:.2f}")

    # Pattern Analysis
    if pattern_analysis:
        message_parts.append(f"\nPattern Analysis:")
        if 'stability_trend' in pattern_analysis:
            trend_direction = "Improving" if pattern_analysis['stability_trend'] > 0 else "Deteriorating"
            message_parts.append(f"- Stability Trend: {trend_direction} ({pattern_analysis['stability_trend']:.3f})")
        if 'stability_volatility' in pattern_analysis:
            message_parts.append(f"- Stability Volatility: {pattern_analysis['stability_volatility']:.3f}")
        if 'seasonal_patterns' in pattern_analysis:
            for metric, pattern in pattern_analysis['seasonal_patterns'].items():
                message_parts.append(f"- {metric} Periodicity: {pattern['period_hours']:.1f} hours")

    # Recommendations
    message_parts.append("\nRecommended Actions:")
    if severity == AlertSeverity.CRITICAL:
        message_parts.append("- IMMEDIATE ACTION REQUIRED: Critical cold chain violation detected!")
        message_parts.append("- Investigate root cause immediately")
        message_parts.append("- Assess product quality")
        message_parts.append("- Consider moving products to backup storage")
    elif severity == AlertSeverity.HIGH:
        message_parts.append("- Urgent attention needed")
        message_parts.append("- Monitor conditions closely")
        message_parts.append("- Prepare backup storage")
    else:
        message_parts.append("- Monitor situation")
        message_parts.append("- Review environmental controls")

    return "\n".join(message_parts)


def generate_alert(anomaly_data: Dict, context: AlertContext) -> Optional[Dict]:
    """Generate comprehensive alert from anomaly detection results"""
    try:
        # Skip if not an anomaly
        if not anomaly_data['is_anomaly']:
            return None

        # Initialize risk assessor
        risk_assessor = RiskAssessor(context)

        # Calculate initial risk score
        base_risk = risk_assessor.calculate_base_risk_score(anomaly_data)

        # Adjust risk based on context
        adjusted_risk = risk_assessor.adjust_risk_for_context(
            base_risk,
            anomaly_data['device_id'],
            anomaly_data
        )

        # Determine alert categories
        categories = risk_assessor.determine_categories(anomaly_data, adjusted_risk)

        # Determine severity
        severity = risk_assessor.determine_severity(adjusted_risk, categories)

        # Get pattern analysis
        pattern_analysis = context.analyze_patterns(
            anomaly_data['device_id'],
            anomaly_data
        )

        # Generate alert message
        message = generate_detailed_message(
            anomaly_data,
            adjusted_risk,
            severity,
            categories,
            pattern_analysis
        )

        # Create alert object
        alert = {
            "alert_id": f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{anomaly_data['device_id']}",
            "timestamp": datetime.now().isoformat(),
            "severity": severity.value,
            "risk_score": adjusted_risk,
            "device_id": anomaly_data['device_id'],
            "categories": categories,
            "window_info": {
                "start": anomaly_data['window_start'],
                "end": anomaly_data['window_end']
            },
            "conditions": {
                "environmental_stability": anomaly_data['environmental_stability'],
                "temperature_analysis": anomaly_data['temperature_analysis'],
                "humidity_analysis": anomaly_data['humidity_analysis'],
                "correlation_analysis": anomaly_data['correlation_analysis'],
                "anomaly_score": anomaly_data['anomaly_score']
            },
            "pattern_analysis": pattern_analysis,
            "message": message
        }

        # Update historical context
        context.update_historical_context(anomaly_data['device_id'], alert)

        return alert

    except Exception as e:
        logger.error(f"Error generating alert: {str(e)}")
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
    """Handle incoming client connections"""
    try:
        start_time = time.perf_counter()
        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        # Initialize alert context
        context = AlertContext()

        # Process each anomaly
        results = []
        for anomaly in input_data['data']:
            alert = generate_alert(anomaly, context)
            if alert is not None:
                results.append(alert)
                if alert['severity'] == 'CRITICAL':
                    logger.warning(f"Critical alert generated for device {alert['device_id']}")

        output = {
            "data": results,
            "total_task_time": input_data['total_task_time'] + time.perf_counter() - start_time
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info(f"Generated {len(results)} alerts")

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
    """Main server function"""
    server = await asyncio.start_server(handle_client, '0.0.0.0', 12345)

    addr = server.sockets[0].getsockname()
    logger.info(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
