# utils/performance_monitor.py
"""Performance monitoring utilities"""

import time
import psutil
import threading
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Monitors system performance during validation"""
    
    def __init__(self):
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'peak_memory_mb': 0,
            'peak_cpu_percent': 0,
            'query_timings': {},
            'system_snapshots': []
        }
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start performance monitoring"""
        self.metrics['start_time'] = datetime.now()
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Performance monitoring started")
    
    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.monitoring = False
        self.metrics['end_time'] = datetime.now()
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Performance monitoring stopped")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # Collect system metrics
                memory = psutil.virtual_memory()
                cpu_percent = psutil.cpu_percent()
                
                # Update peak values
                memory_mb = memory.used / (1024 * 1024)
                self.metrics['peak_memory_mb'] = max(self.metrics['peak_memory_mb'], memory_mb)
                self.metrics['peak_cpu_percent'] = max(self.metrics['peak_cpu_percent'], cpu_percent)
                
                # Store snapshot
                snapshot = {
                    'timestamp': datetime.now(),
                    'memory_mb': memory_mb,
                    'memory_percent': memory.percent,
                    'cpu_percent': cpu_percent,
                    'disk_usage': psutil.disk_usage('/').percent
                }
                self.metrics['system_snapshots'].append(snapshot)
                
                # Keep only last 1000 snapshots to prevent memory bloat
                if len(self.metrics['system_snapshots']) > 1000:
                    self.metrics['system_snapshots'].pop(0)
                
                time.sleep(5)  # Monitor every 5 seconds
                
            except Exception as e:
                logger.warning(f"Performance monitoring error: {e}")
    
    def record_query_timing(self, query_id: str, start_time: float, end_time: float, 
                           rows_processed: int = 0):
        """Record timing for a specific query"""
        duration = end_time - start_time
        self.metrics['query_timings'][query_id] = {
            'duration': duration,
            'start_time': datetime.fromtimestamp(start_time),
            'end_time': datetime.fromtimestamp(end_time),
            'rows_processed': rows_processed,
            'rows_per_second': rows_processed / duration if duration > 0 else 0
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        if self.metrics['start_time'] and self.metrics['end_time']:
            total_duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
        else:
            total_duration = 0
        
        return {
            'total_duration_seconds': total_duration,
            'peak_memory_mb': self.metrics['peak_memory_mb'],
            'peak_cpu_percent': self.metrics['peak_cpu_percent'],
            'total_queries': len(self.metrics['query_timings']),
            'average_query_duration': sum(q['duration'] for q in self.metrics['query_timings'].values()) / 
                                    len(self.metrics['query_timings']) if self.metrics['query_timings'] else 0,
            'total_rows_processed': sum(q['rows_processed'] for q in self.metrics['query_timings'].values())
        }
