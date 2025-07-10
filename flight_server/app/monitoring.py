"""
Monitoring and observability utilities for DataHut-DuckHouse.
"""
import time
import logging
from typing import Optional, Dict, Any
from functools import wraps
from contextlib import contextmanager

import psutil
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer


logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collect and track application metrics."""
    
    def __init__(self):
        self.metrics = {
            "requests_total": 0,
            "requests_failed": 0,
            "response_times": [],
            "active_connections": 0,
            "data_volume_processed": 0,
            "tables_created": 0,
            "tables_updated": 0,
        }
    
    def increment_counter(self, metric_name: str, value: int = 1) -> None:
        """Increment a counter metric."""
        if metric_name in self.metrics:
            self.metrics[metric_name] += value
        else:
            logger.warning(f"Unknown metric: {metric_name}")
    
    def record_response_time(self, response_time: float) -> None:
        """Record a response time measurement."""
        self.metrics["response_times"].append(response_time)
        # Keep only last 1000 measurements
        if len(self.metrics["response_times"]) > 1000:
            self.metrics["response_times"] = self.metrics["response_times"][-1000:]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        metrics = self.metrics.copy()
        
        # Add computed metrics
        if self.metrics["response_times"]:
            metrics["avg_response_time"] = sum(self.metrics["response_times"]) / len(self.metrics["response_times"])
            metrics["max_response_time"] = max(self.metrics["response_times"])
            metrics["min_response_time"] = min(self.metrics["response_times"])
        
        # Add system metrics
        metrics["cpu_percent"] = psutil.cpu_percent()
        metrics["memory_percent"] = psutil.virtual_memory().percent
        metrics["disk_usage_percent"] = psutil.disk_usage('/').percent
        
        return metrics


class TracingSetup:
    """Setup distributed tracing with OpenTelemetry."""
    
    def __init__(self, service_name: str = "datahut-duckhouse"):
        self.service_name = service_name
        self.tracer_provider = None
        self.tracer = None
    
    def setup_tracing(self, jaeger_endpoint: Optional[str] = None) -> None:
        """Setup distributed tracing."""
        try:
            # Set up tracer provider
            self.tracer_provider = TracerProvider()
            trace.set_tracer_provider(self.tracer_provider)
            
            # Set up Jaeger exporter if endpoint provided
            if jaeger_endpoint:
                jaeger_exporter = JaegerExporter(
                    agent_host_name=jaeger_endpoint,
                    agent_port=6831,
                )
                span_processor = BatchSpanProcessor(jaeger_exporter)
                self.tracer_provider.add_span_processor(span_processor)
            
            # Get tracer
            self.tracer = trace.get_tracer(self.service_name)
            
            # Instrument gRPC
            GrpcInstrumentorServer().instrument()
            
            logger.info(f"Tracing setup completed for service: {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup tracing: {e}")
    
    def get_tracer(self):
        """Get the tracer instance."""
        return self.tracer


# Global instances
metrics_collector = MetricsCollector()
tracing_setup = TracingSetup()


def monitor_performance(func):
    """Decorator to monitor function performance."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        metrics_collector.increment_counter("requests_total")
        
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            metrics_collector.increment_counter("requests_failed")
            logger.error(f"Error in {func.__name__}: {e}")
            raise
        finally:
            end_time = time.time()
            response_time = end_time - start_time
            metrics_collector.record_response_time(response_time)
    
    return wrapper


@contextmanager
def trace_operation(operation_name: str, attributes: Optional[Dict[str, Any]] = None):
    """Context manager for tracing operations."""
    tracer = tracing_setup.get_tracer()
    if not tracer:
        yield
        return
    
    with tracer.start_as_current_span(operation_name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise


def setup_monitoring(jaeger_endpoint: Optional[str] = None) -> None:
    """Setup monitoring and observability."""
    # Setup tracing
    tracing_setup.setup_tracing(jaeger_endpoint)
    
    # Setup structured logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/application.log')
        ]
    )
    
    logger.info("Monitoring setup completed")


def get_health_status() -> Dict[str, Any]:
    """Get application health status."""
    try:
        # Check system resources
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Determine health status
        is_healthy = (
            cpu_percent < 80 and
            memory.percent < 80 and
            disk.percent < 90
        )
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": time.time(),
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk.percent,
                "load_average": psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
            },
            "metrics": metrics_collector.get_metrics()
        }
    
    except Exception as e:
        logger.error(f"Failed to get health status: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
        }
