"""
AI-powered email categorization using LLM.
Categorizes emails for credit control purposes.
"""

import json
import logging
import re
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class EmailCategorizer:
    """
    Uses LLM to categorize emails for credit control workflows.
    """

    CATEGORIES = {
        'payment': 'Payment related - remittance advice, payment confirmation, payment queries, payment promises',
        'query': 'General inquiry - account questions, balance queries, statement requests, invoice copies',
        'complaint': 'Complaint or dispute - invoice disputes, pricing issues, service problems, product issues',
        'order': 'Order related - new orders, order confirmations, order status, delivery queries',
        'other': 'Other correspondence not fitting above categories'
    }

    PROMPT_TEMPLATE = """Analyze this email and categorize it for credit control purposes.

Email Details:
- Subject: {subject}
- From: {from_address}
- Body:
{body}

Available Categories:
- payment: {payment_desc}
- query: {query_desc}
- complaint: {complaint_desc}
- order: {order_desc}
- other: {other_desc}

Instructions:
1. Read the email content carefully
2. Determine which category best fits the email's primary purpose
3. Provide a confidence score from 0.0 to 1.0
4. Give a brief reason for your classification

Respond with ONLY a valid JSON object in this exact format:
{{"category": "<category>", "confidence": <number>, "reason": "<brief explanation>"}}

JSON Response:"""

    def __init__(self, llm=None):
        """
        Initialize the categorizer.

        Args:
            llm: LLM instance for generating completions (optional, can be set later)
        """
        self.llm = llm

    def set_llm(self, llm):
        """Set the LLM instance."""
        self.llm = llm

    def categorize(
        self,
        subject: str,
        from_address: str,
        body: str
    ) -> Dict[str, Any]:
        """
        Categorize an email using LLM.

        Args:
            subject: Email subject
            from_address: Sender email address
            body: Email body text (preview or full)

        Returns:
            Dictionary with 'category', 'confidence', and 'reason'
        """
        if not self.llm:
            logger.warning("No LLM configured for email categorization")
            return {
                'category': 'uncategorized',
                'confidence': 0.0,
                'reason': 'LLM not configured'
            }

        try:
            # Truncate body if too long
            body_truncated = body[:1500] if body else ""

            # Build prompt
            prompt = self.PROMPT_TEMPLATE.format(
                subject=subject or "(No subject)",
                from_address=from_address,
                body=body_truncated or "(No body content)",
                payment_desc=self.CATEGORIES['payment'],
                query_desc=self.CATEGORIES['query'],
                complaint_desc=self.CATEGORIES['complaint'],
                order_desc=self.CATEGORIES['order'],
                other_desc=self.CATEGORIES['other']
            )

            # Get LLM response
            response = self.llm.get_completion(prompt, temperature=0.1)

            # Parse JSON response
            result = self._parse_response(response)

            # Validate category
            if result['category'] not in self.CATEGORIES:
                result['category'] = 'other'
                result['reason'] = f"Invalid category returned, defaulting to 'other'. Original: {result.get('reason', '')}"

            logger.info(f"Categorized email '{subject[:50]}...' as {result['category']} (confidence: {result['confidence']})")
            return result

        except Exception as e:
            logger.error(f"Error categorizing email: {e}")
            return {
                'category': 'uncategorized',
                'confidence': 0.0,
                'reason': f'Error during categorization: {str(e)}'
            }

    def _parse_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM response to extract category information."""
        # Try to extract JSON from response
        try:
            # First, try direct JSON parse
            return json.loads(response.strip())
        except json.JSONDecodeError:
            pass

        # Try to find JSON object in response
        json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass

        # Try to extract fields manually
        result = {
            'category': 'other',
            'confidence': 0.5,
            'reason': 'Could not parse LLM response'
        }

        # Look for category keyword
        response_lower = response.lower()
        for category in self.CATEGORIES.keys():
            if category in response_lower:
                result['category'] = category
                break

        # Try to extract confidence
        confidence_match = re.search(r'confidence[:\s]+(\d+\.?\d*)', response_lower)
        if confidence_match:
            try:
                result['confidence'] = min(1.0, float(confidence_match.group(1)))
            except ValueError:
                pass

        return result

    def categorize_batch(
        self,
        emails: list
    ) -> list:
        """
        Categorize multiple emails.

        Args:
            emails: List of dicts with 'subject', 'from_address', 'body'

        Returns:
            List of categorization results
        """
        results = []
        for email in emails:
            result = self.categorize(
                subject=email.get('subject', ''),
                from_address=email.get('from_address', ''),
                body=email.get('body', email.get('body_preview', ''))
            )
            results.append(result)
        return results


class CustomerLinker:
    """
    Links emails to customer accounts based on email address matching.
    """

    def __init__(self, sql_connector=None):
        """
        Initialize the customer linker.

        Args:
            sql_connector: SQL connector for database queries (optional, can be set later)
        """
        self.sql_connector = sql_connector
        self._email_cache: Dict[str, Optional[str]] = {}

    def set_sql_connector(self, sql_connector):
        """Set the SQL connector instance."""
        self.sql_connector = sql_connector

    def find_customer_by_email(self, email_address: str) -> Optional[Dict[str, Any]]:
        """
        Find customer account by email address.

        Args:
            email_address: Email address to search for

        Returns:
            Dictionary with account info or None if not found
        """
        if not self.sql_connector:
            logger.warning("No SQL connector configured for customer linking")
            return None

        if not email_address:
            return None

        email_lower = email_address.lower().strip()

        # Check cache first
        if email_lower in self._email_cache:
            cached = self._email_cache[email_lower]
            if cached:
                return {'sn_account': cached, 'from_cache': True}
            return None

        try:
            # Query sname table for matching email
            query = """
                SELECT sn_account, sn_name, sn_email, sn_currbal
                FROM sname
                WHERE LOWER(sn_email) = ?
            """
            result = self.sql_connector.execute_query(query, [email_lower])

            # Handle DataFrame or list result
            if hasattr(result, 'to_dict'):
                result = result.to_dict('records')

            if result and len(result) > 0:
                account = result[0]['sn_account']
                self._email_cache[email_lower] = account
                return {
                    'sn_account': account,
                    'sn_name': result[0].get('sn_name', ''),
                    'sn_email': result[0].get('sn_email', ''),
                    'sn_currbal': result[0].get('sn_currbal', 0)
                }

            # Try domain matching as fallback
            domain = email_address.split('@')[1] if '@' in email_address else None
            if domain:
                query = """
                    SELECT sn_account, sn_name, sn_email, sn_currbal
                    FROM sname
                    WHERE sn_email LIKE ?
                    AND sn_currbal > 0
                    ORDER BY sn_currbal DESC
                """
                result = self.sql_connector.execute_query(query, [f'%@{domain}'])

                if hasattr(result, 'to_dict'):
                    result = result.to_dict('records')

                # Only use domain match if there's exactly one result
                if result and len(result) == 1:
                    account = result[0]['sn_account']
                    self._email_cache[email_lower] = account
                    return {
                        'sn_account': account,
                        'sn_name': result[0].get('sn_name', ''),
                        'sn_email': result[0].get('sn_email', ''),
                        'sn_currbal': result[0].get('sn_currbal', 0),
                        'domain_match': True
                    }

            # Cache negative result
            self._email_cache[email_lower] = None
            return None

        except Exception as e:
            logger.error(f"Error finding customer by email: {e}")
            return None

    def get_customer_name(self, account_code: str) -> Optional[str]:
        """Get customer name by account code."""
        if not self.sql_connector or not account_code:
            return None

        try:
            query = "SELECT sn_name FROM sname WHERE sn_account = ?"
            result = self.sql_connector.execute_query(query, [account_code])

            if hasattr(result, 'to_dict'):
                result = result.to_dict('records')

            if result and len(result) > 0:
                return result[0].get('sn_name', '')

            return None
        except Exception as e:
            logger.error(f"Error getting customer name: {e}")
            return None

    def clear_cache(self):
        """Clear the email lookup cache."""
        self._email_cache.clear()
