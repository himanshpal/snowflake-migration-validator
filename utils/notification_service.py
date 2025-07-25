# utils/notification_service.py
"""Notification service for alerts and reports"""

import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class NotificationService:
    """Handles email and Slack notifications"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.smtp_config = self.config.get('smtp', {})
        self.slack_config = self.config.get('slack', {})
    
    def send_email(self, subject: str, body: str, recipients: List[str], 
                   html_body: Optional[str] = None):
        """Send email notification"""
        try:
            if not self.smtp_config.get('server'):
                logger.warning("SMTP not configured, skipping email notification")
                return
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_config.get('username', 'noreply@example.com')
            msg['To'] = ', '.join(recipients)
            
            # Add text part
            text_part = MIMEText(body, 'plain')
            msg.attach(text_part)
            
            # Add HTML part if provided
            if html_body:
                html_part = MIMEText(html_body, 'html')
                msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_config['server'], self.smtp_config.get('port', 587)) as server:
                server.starttls()
                server.login(self.smtp_config['username'], self.smtp_config['password'])
                server.send_message(msg)
            
            logger.info(f"Email sent to {len(recipients)} recipients")
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
    
    def send_slack_message(self, message: str, channel: Optional[str] = None):
        """Send Slack notification"""
        try:
            webhook_url = self.slack_config.get('webhook_url')
            if not webhook_url:
                logger.warning("Slack webhook not configured, skipping notification")
                return
            
            payload = {
                'text': message,
                'username': 'Snowflake Validator',
                'icon_emoji': ':snowflake:'
            }
            
            if channel:
                payload['channel'] = channel
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Slack notification sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
    
    def notify_validation_start(self, execution_id: str, total_queries: int):
        """Notify validation start"""
        message = f"🚀 Snowflake validation started\nExecution ID: {execution_id}\nTotal queries: {total_queries}"
        
        self.send_slack_message(message)
        
        recipients = self.config.get('notification_recipients', [])
        if recipients:
            self.send_email(
                subject=f"Snowflake Validation Started - {execution_id}",
                body=message,
                recipients=recipients
            )
    
    def notify_validation_complete(self, summary: Dict[str, Any]):
        """Notify validation completion"""
        success_rate = summary.get('success_rate', 0)
        status_emoji = "✅" if success_rate == 100 else "⚠️" if success_rate >= 90 else "❌"
        
        message = f"""{status_emoji} Snowflake validation completed
Execution ID: {summary.get('execution_id')}
Success Rate: {success_rate:.1f}%
Completed: {summary.get('completed_queries', 0)}
Failed: {summary.get('failed_queries', 0)}
Total: {summary.get('total_queries', 0)}"""
        
        self.send_slack_message(message)
        
        recipients = self.config.get('notification_recipients', [])
        if recipients:
            self.send_email(
                subject=f"Snowflake Validation Complete - {summary.get('execution_id')}",
                body=message,
                recipients=recipients
            )
