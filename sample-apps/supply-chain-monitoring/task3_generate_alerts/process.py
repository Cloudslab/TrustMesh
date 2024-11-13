import asyncio
import json
import logging
import time
import traceback
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class AlertCategory(Enum):
    STABILITY_BREACH = "STABILITY_BREACH"
    ANOMALY_DETECTED = "ANOMALY_DETECTED"
    ENVIRONMENTAL_DEVIATION = "ENVIRONMENTAL_DEVIATION"
    SYSTEM_WARNING = "SYSTEM_WARNING"


class AlertContext:
    def __init__(self):
        self.device_stats = {}

    def update_device_stats(self, device_id: str, alert_data: Dict):
        """Update statistics for a device"""
        if device_id not in self.device_stats:
            self.device_stats[device_id] = {
                'total_alerts': 0,
                'recent_scores': [],
                'max_score': 0.0
            }

        stats = self.device_stats[device_id]
        stats['total_alerts'] += 1
        stats['recent_scores'].append(alert_data['anomaly_score'])
        stats['max_score'] = max(stats['max_score'], alert_data['anomaly_score'])

        # Keep only recent scores
        if len(stats['recent_scores']) > 10:
            stats['recent_scores'].pop(0)

    def get_device_stats(self, device_id: str) -> Dict:
        """Get statistics for a device"""
        if device_id not in self.device_stats:
            return {
                'total_alerts': 0,
                'recent_scores': [],
                'max_score': 0.0,
                'trend': 0.0
            }

        stats = self.device_stats[device_id]

        # Calculate score trend
        if len(stats['recent_scores']) >= 2:
            trend = stats['recent_scores'][-1] - stats['recent_scores'][0]
        else:
            trend = 0.0

        return {
            'total_alerts': stats['total_alerts'],
            'recent_scores': stats['recent_scores'],
            'max_score': stats['max_score'],
            'trend': trend
        }


class RiskAssessor:
    def calculate_risk_score(self, anomaly_data: Dict) -> float:
        """Calculate risk score based on anomaly detection results"""
        try:
            weights = {
                'anomaly_score': 0.4,
                'stability': 0.3,
                'feature_stats': 0.3
            }

            # Get anomaly score
            anomaly_score = anomaly_data.get('anomaly_score', 0.0)

            # Get stability score (inverse of environmental stability)
            stability = anomaly_data.get('environmental_stability', 0.0)
            stability_risk = 1.0 - (stability / 100.0)

            # Get feature statistics risk
            feature_analysis = anomaly_data.get('feature_analysis', {})
            feature_mean = feature_analysis.get('feature_mean', 0.0)
            feature_std = feature_analysis.get('feature_std', 0.0)
            feature_risk = min(1.0, abs(feature_mean) + feature_std)

            # Combine scores
            risk_score = (
                    weights['anomaly_score'] * anomaly_score +
                    weights['stability'] * stability_risk +
                    weights['feature_stats'] * feature_risk
            )

            return min(max(risk_score, 0.0), 1.0)

        except Exception as e:
            logger.error(f"Error calculating risk score: {str(e)}")
            return 1.0  # Return maximum risk on error

    def adjust_risk_for_context(self, base_risk: float, device_stats: Dict) -> float:
        """Adjust risk score based on historical context"""
        try:
            # Start with base risk
            adjusted_risk = base_risk

            # Adjust for alert frequency
            if device_stats['total_alerts'] > 5:
                adjusted_risk *= 1.2

            # Adjust for trend
            if device_stats['trend'] > 0:  # Worsening trend
                adjusted_risk *= 1.1

            # Adjust for historical severity
            if device_stats['max_score'] > 0.8:
                adjusted_risk *= 1.1

            return min(max(adjusted_risk, 0.0), 1.0)

        except Exception as e:
            logger.error(f"Error adjusting risk score: {str(e)}")
            return base_risk


def determine_severity(risk_score: float) -> AlertSeverity:
    """Determine alert severity based on risk score"""
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


def determine_categories(anomaly_data: Dict, risk_score: float) -> list:
    """Determine applicable alert categories"""
    categories = []

    # Check stability breach
    if anomaly_data.get('environmental_stability', 100) < 70:
        categories.append(AlertCategory.STABILITY_BREACH.value)

    # Check anomaly detection
    if anomaly_data.get('is_anomaly', False):
        categories.append(AlertCategory.ANOMALY_DETECTED.value)

    # Check high risk score
    if risk_score > 0.6:
        categories.append(AlertCategory.ENVIRONMENTAL_DEVIATION.value)

    # Add system warning if needed
    if not categories:
        categories.append(AlertCategory.SYSTEM_WARNING.value)

    return categories


def generate_message(anomaly_data: Dict, risk_score: float, severity: AlertSeverity, categories: list) -> str:
    """Generate detailed alert message"""
    messages = []

    # Header
    messages.append(f"Cold Chain Alert - {severity.value}")
    messages.append(f"Device: {anomaly_data['device_id']}")
    messages.append(f"Time Window: {anomaly_data['window_start']} to {anomaly_data['window_end']}")
    messages.append(f"Risk Score: {risk_score:.2f}")

    # Categories
    messages.append("\nAlert Categories:")
    for category in categories:
        messages.append(f"- {category}")

    # Environmental Stability
    messages.append(f"\nEnvironmental Stability: {anomaly_data.get('environmental_stability', 'N/A')}%")

    # Anomaly Information
    messages.append("\nAnomaly Analysis:")
    messages.append(f"- Anomaly Score: {anomaly_data.get('anomaly_score', 'N/A')}")
    messages.append(f"- Anomaly Detected: {anomaly_data.get('is_anomaly', 'N/A')}")

    # Feature Analysis
    feature_analysis = anomaly_data.get('feature_analysis', {})
    if feature_analysis:
        messages.append("\nFeature Analysis:")
        messages.append(f"- Feature Count: {feature_analysis.get('feature_count', 'N/A')}")
        messages.append(f"- Feature Mean: {feature_analysis.get('feature_mean', 'N/A'):.2f}")
        messages.append(f"- Feature Std: {feature_analysis.get('feature_std', 'N/A'):.2f}")

    # Recommendations
    messages.append("\nRecommended Actions:")
    if severity == AlertSeverity.CRITICAL:
        messages.append("- IMMEDIATE ACTION REQUIRED")
        messages.append("- Verify cold chain integrity")
        messages.append("- Check environmental controls")
        messages.append("- Prepare for product inspection")
    elif severity == AlertSeverity.HIGH:
        messages.append("- Monitor situation closely")
        messages.append("- Review environmental controls")
        messages.append("- Prepare contingency measures")
    else:
        messages.append("- Continue monitoring")
        messages.append("- Document conditions")

    return "\n".join(messages)


def generate_alert(anomaly_data: Dict, context: AlertContext) -> Optional[Dict]:
    """Generate alert from anomaly detection results"""
    try:
        # Initialize risk assessor
        risk_assessor = RiskAssessor()

        # Calculate risk scores
        base_risk = risk_assessor.calculate_risk_score(anomaly_data)

        # Get device statistics
        device_stats = context.get_device_stats(anomaly_data['device_id'])

        # Adjust risk based on context
        adjusted_risk = risk_assessor.adjust_risk_for_context(base_risk, device_stats)

        # Determine severity and categories
        severity = determine_severity(adjusted_risk)
        categories = determine_categories(anomaly_data, adjusted_risk)

        # Generate alert message
        message = generate_message(anomaly_data, adjusted_risk, severity, categories)

        # Create alert
        alert = {
            "alert_id": f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{anomaly_data['device_id']}",
            "timestamp": datetime.now().isoformat(),
            "severity": severity.value,
            "risk_score": adjusted_risk,
            "categories": categories,
            "device_id": anomaly_data['device_id'],
            "window_info": {
                "start": anomaly_data['window_start'],
                "end": anomaly_data['window_end']
            },
            "conditions": {
                "environmental_stability": anomaly_data.get('environmental_stability', None),
                "anomaly_score": anomaly_data.get('anomaly_score', None),
                "is_anomaly": anomaly_data.get('is_anomaly', None),
                "feature_analysis": anomaly_data.get('feature_analysis', {})
            },
            "message": message
        }

        # Update context
        context.update_device_stats(anomaly_data['device_id'], anomaly_data)

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
