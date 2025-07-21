import asyncio
import json
import logging
import time
import traceback
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models
import pickle
import base64
from sklearn.metrics import classification_report, confusion_matrix
from typing import Dict, Any, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MNISTModelEvaluator:
    def __init__(self):
        self.model = None
        self.test_data = None
        
    def create_model(self):
        """Create the same CNN model architecture as used in training"""
        model = models.Sequential([
            layers.Conv2D(32, (3, 3), activation='relu', input_shape=(28, 28, 1)),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.Flatten(),
            layers.Dense(64, activation='relu'),
            layers.Dense(10, activation='softmax')
        ])
        
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        return model
    
    def load_test_data(self):
        """Load MNIST test dataset for evaluation"""
        try:
            # Load MNIST test data
            (_, _), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
            
            # Preprocess the data
            x_test = x_test.reshape(-1, 28, 28, 1).astype('float32') / 255.0
            
            logger.info(f"Loaded test data: {x_test.shape} samples")
            return x_test, y_test
            
        except Exception as e:
            logger.error(f"Error loading test data: {e}")
            raise
    
    def load_model_weights(self, encoded_weights: str):
        """Load aggregated model weights"""
        try:
            # Decode weights
            weights_data = base64.b64decode(encoded_weights)
            weights = pickle.loads(weights_data)
            
            # Create model and set weights
            self.model = self.create_model()
            self.model.set_weights(weights)
            
            logger.info("Loaded aggregated model weights for evaluation")
            
        except Exception as e:
            logger.error(f"Error loading model weights: {e}")
            raise
    
    def evaluate_model(self, x_test: np.ndarray, y_test: np.ndarray) -> Dict[str, Any]:
        """Evaluate the federated model on test dataset"""
        try:
            start_time = time.time()
            
            # Evaluate model on test data
            test_loss, test_accuracy = self.model.evaluate(x_test, y_test, verbose=0)
            
            # Get predictions
            predictions = self.model.predict(x_test, verbose=0)
            predicted_classes = np.argmax(predictions, axis=1)
            
            # Calculate detailed metrics
            conf_matrix = confusion_matrix(y_test, predicted_classes)
            class_report = classification_report(y_test, predicted_classes, output_dict=True)
            
            # Calculate per-class accuracy
            per_class_accuracy = {}
            for i in range(10):
                class_mask = (y_test == i)
                if np.sum(class_mask) > 0:
                    class_acc = np.mean(predicted_classes[class_mask] == y_test[class_mask])
                    per_class_accuracy[f'class_{i}'] = float(class_acc)
            
            # Calculate top-5 accuracy
            top5_predictions = tf.nn.top_k(predictions, k=5).indices.numpy()
            top5_accuracy = np.mean([y_test[i] in top5_predictions[i] for i in range(len(y_test))])
            
            evaluation_time = time.time() - start_time
            
            metrics = {
                'test_loss': float(test_loss),
                'test_accuracy': float(test_accuracy),
                'top5_accuracy': float(top5_accuracy),
                'per_class_accuracy': per_class_accuracy,
                'confusion_matrix': conf_matrix.tolist(),
                'classification_report': class_report,
                'evaluation_time': evaluation_time,
                'total_test_samples': len(x_test)
            }
            
            logger.info(f"Model evaluation completed in {evaluation_time:.2f}s")
            logger.info(f"Test accuracy: {test_accuracy:.4f}, Test loss: {test_loss:.4f}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error evaluating model: {e}")
            raise
    
    def analyze_federated_performance(self, evaluation_metrics: Dict, aggregation_metrics: Dict) -> Dict[str, Any]:
        """Analyze federated learning performance"""
        try:
            analysis = {
                'federated_training_summary': {
                    'nodes_participated': aggregation_metrics.get('num_clients', 0),
                    'total_training_samples': aggregation_metrics.get('total_samples', 0),
                    'avg_training_loss': aggregation_metrics.get('avg_loss', 0),
                    'avg_training_accuracy': aggregation_metrics.get('avg_accuracy', 0)
                },
                'global_model_performance': {
                    'test_accuracy': evaluation_metrics['test_accuracy'],
                    'test_loss': evaluation_metrics['test_loss'],
                    'top5_accuracy': evaluation_metrics['top5_accuracy']
                },
                'privacy_analysis': {
                    'data_kept_local': True,
                    'only_model_parameters_shared': True,
                    'no_raw_data_exchange': True
                },
                'convergence_analysis': self._analyze_convergence(evaluation_metrics),
                'federated_learning_benefits': self._assess_fl_benefits(evaluation_metrics, aggregation_metrics)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing federated performance: {e}")
            return {}
    
    def _analyze_convergence(self, metrics: Dict) -> Dict[str, Any]:
        """Analyze model convergence"""
        test_accuracy = metrics['test_accuracy']
        
        convergence = {
            'model_quality': 'Excellent' if test_accuracy > 0.95 else 
                           'Good' if test_accuracy > 0.90 else 
                           'Fair' if test_accuracy > 0.80 else 'Needs Improvement',
            'performance_grade': self._calculate_performance_grade(test_accuracy),
            'ready_for_production': test_accuracy > 0.90
        }
        
        return convergence
    
    def _assess_fl_benefits(self, eval_metrics: Dict, agg_metrics: Dict) -> Dict[str, Any]:
        """Assess benefits of federated learning approach"""
        benefits = {
            'data_diversity': f"Trained on {agg_metrics.get('num_clients', 0)} different data distributions",
            'sample_efficiency': f"Leveraged {agg_metrics.get('total_samples', 0)} samples across nodes",
            'privacy_preservation': "Raw data never left individual nodes",
            'distributed_computation': "Training load distributed across multiple devices",
            'model_robustness': "Model exposed to diverse local data patterns"
        }
        
        # Calculate efficiency metrics
        total_samples = agg_metrics.get('total_samples', 0)
        test_accuracy = eval_metrics['test_accuracy']
        
        if total_samples > 0:
            benefits['sample_to_accuracy_ratio'] = f"{test_accuracy:.4f} accuracy with {total_samples} samples"
        
        return benefits
    
    def _calculate_performance_grade(self, accuracy: float) -> str:
        """Calculate performance grade based on accuracy"""
        if accuracy >= 0.95:
            return "A+ (Excellent)"
        elif accuracy >= 0.90:
            return "A (Very Good)"
        elif accuracy >= 0.85:
            return "B (Good)"
        elif accuracy >= 0.80:
            return "C (Fair)"
        else:
            return "D (Needs Improvement)"
    
    def generate_evaluation_report(self, metrics: Dict[str, Any], 
                                 aggregation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive evaluation report"""
        try:
            # Extract aggregation metrics
            aggregation_metrics = aggregation_data.get('aggregation_metrics', {})
            round_number = aggregation_data.get('round_number', 1)
            
            # Perform federated learning analysis
            fl_analysis = self.analyze_federated_performance(metrics, aggregation_metrics)
            
            report = {
                'evaluation_summary': {
                    'round_number': round_number,
                    'evaluation_timestamp': time.time(),
                    'test_accuracy': metrics['test_accuracy'],
                    'test_loss': metrics['test_loss'],
                    'performance_grade': fl_analysis['convergence_analysis']['performance_grade']
                },
                'detailed_metrics': {
                    'per_class_accuracy': metrics['per_class_accuracy'],
                    'classification_report': metrics['classification_report'],
                    'confusion_matrix': metrics['confusion_matrix'],
                    'top5_accuracy': metrics['top5_accuracy']
                },
                'federated_learning_analysis': fl_analysis,
                'recommendations': self._generate_recommendations(metrics, aggregation_metrics),
                'next_round_suggestions': self._suggest_next_round_improvements(metrics)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating evaluation report: {e}")
            return {}
    
    def _generate_recommendations(self, eval_metrics: Dict, agg_metrics: Dict) -> List[str]:
        """Generate recommendations based on evaluation results"""
        recommendations = []
        
        test_accuracy = eval_metrics['test_accuracy']
        per_class_acc = eval_metrics['per_class_accuracy']
        
        if test_accuracy < 0.85:
            recommendations.append("Consider increasing local training epochs or learning rate")
        
        if test_accuracy > 0.95:
            recommendations.append("Excellent performance! Model is ready for deployment")
        
        # Check class imbalance
        class_accuracies = list(per_class_acc.values())
        if max(class_accuracies) - min(class_accuracies) > 0.15:
            recommendations.append("Address class imbalance - some digits perform significantly worse")
        
        # Check if all nodes participated
        num_clients = agg_metrics.get('num_clients', 0)
        if num_clients < 5:
            recommendations.append(f"Only {num_clients}/5 nodes participated - ensure all nodes are active")
        
        return recommendations
    
    def _suggest_next_round_improvements(self, metrics: Dict) -> List[str]:
        """Suggest improvements for next federated learning round"""
        suggestions = []
        
        test_accuracy = metrics['test_accuracy']
        
        if test_accuracy < 0.90:
            suggestions.append("Increase local training epochs from 5 to 10")
            suggestions.append("Consider data augmentation for better generalization")
        
        if test_accuracy > 0.93:
            suggestions.append("Model converging well - consider reducing learning rate for fine-tuning")
        
        suggestions.append("Monitor for overfitting on local data distributions")
        suggestions.append("Consider implementing differential privacy for stronger privacy guarantees")
        
        return suggestions
    
    def process_evaluation_request(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main processing function for model evaluation"""
        try:
            start_time = time.time()
            
            # Extract data from input
            aggregation_data = input_data.get('data', [{}])[0]
            global_weights = aggregation_data.get('global_model_weights')
            
            if not global_weights:
                raise ValueError("No global model weights found in input data")
            
            round_number = aggregation_data.get('round_number', 1)
            logger.info(f"Starting model evaluation for round {round_number}")
            
            # Load model weights
            self.load_model_weights(global_weights)
            
            # Load test data
            x_test, y_test = self.load_test_data()
            
            # Evaluate model
            evaluation_metrics = self.evaluate_model(x_test, y_test)
            
            # Generate comprehensive report
            evaluation_report = self.generate_evaluation_report(evaluation_metrics, aggregation_data)
            
            processing_time = time.time() - start_time
            
            result = {
                'round_number': round_number,
                'evaluation_report': evaluation_report,
                'evaluation_metrics': evaluation_metrics,
                'processing_time': processing_time,
                'data': [evaluation_report]  # Pass report to next stage if needed
            }
            
            logger.info(f"✅ Model evaluation completed for round {round_number} in {processing_time:.2f}s")
            logger.info(f"🎯 Final Results - Accuracy: {evaluation_metrics['test_accuracy']:.4f}, "
                       f"Loss: {evaluation_metrics['test_loss']:.4f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in processing evaluation request: {e}")
            raise

async def read_json(reader):
    """Read JSON data from stream"""
    try:
        data = await reader.read(1024 * 1024 * 10)  # 10MB buffer
        if not data:
            return None
        
        json_str = data.decode('utf-8')
        return json.loads(json_str)
        
    except Exception as e:
        logger.error(f"Error reading JSON: {e}")
        return None

async def handle_client(reader, writer):
    """Handle client connections for evaluation requests"""
    client_addr = writer.get_extra_info('peername')
    logger.info(f"Client connected from {client_addr}")
    
    try:
        # Read input data
        input_data = await read_json(reader)
        if input_data is None:
            raise ValueError("No input data received")
        
        # Process evaluation request
        evaluator = MNISTModelEvaluator()
        result = evaluator.process_evaluation_request(input_data)
        
        # Add total task time from input
        total_task_time = input_data.get('total_task_time', 0.0)
        result['total_task_time'] = total_task_time + result['processing_time']
        
        # Send response
        response = json.dumps(result, indent=2)
        writer.write(response.encode('utf-8'))
        await writer.drain()
        
        logger.info(f"Successfully processed evaluation request from {client_addr}")
        
    except Exception as e:
        logger.error(f"Error handling client {client_addr}: {e}")
        logger.error(traceback.format_exc())
        
        # Send error response
        error_response = {
            'error': str(e),
            'total_task_time': input_data.get('total_task_time', 0.0) if 'input_data' in locals() else 0.0
        }
        response = json.dumps(error_response)
        writer.write(response.encode('utf-8'))
        await writer.drain()
        
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    """Start the evaluation server"""
    logger.info("Starting MNIST Model Evaluation Server on port 12345")
    
    server = await asyncio.start_server(
        handle_client, 
        '0.0.0.0', 
        12345
    )
    
    addr = server.sockets[0].getsockname()
    logger.info(f"Evaluation server running on {addr[0]}:{addr[1]}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())