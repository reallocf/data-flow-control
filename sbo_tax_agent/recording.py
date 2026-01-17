"""
Recording utility for saving LLM responses to files.

Creates timestamped directories and saves responses with descriptive filenames
to track when during app execution each response was made.

The recorder organizes files into session directories with subdirectories:
- agent_loop/: Agent loop requests and responses (transaction processing)
- llm_resolution/: LLM resolution requests and responses (policy violation fixes)

Each file includes:
- Sequence number for ordering
- Timestamp for when the request/response occurred
- Transaction ID and iteration number (for agent loop)
- Full request/response JSON data

Usage:
    from recording import LLMRecorder
    
    recorder = LLMRecorder(base_dir="session_records")
    if recorder.is_enabled():
        recorder.record_agent_loop_request(
            transaction_id=1,
            iteration=1,
            request_body={...}
        )
"""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path


class LLMRecorder:
    """Records LLM responses to files with timestamps and sequence numbers.
    
    Creates organized session directories with timestamped subdirectories for
    agent loop messages and LLM resolution responses. Each recorded file includes
    metadata like sequence numbers, timestamps, transaction IDs, and full request/response data.
    
    Example:
        recorder = LLMRecorder(base_dir="session_records")
        # Creates: session_records/session_20260117_100205/
        #          session_records/session_20260117_100205/agent_loop/
        #          session_records/session_20260117_100205/llm_resolution/
    """
    
    def __init__(self, base_dir: Optional[str] = None):
        """Initialize the recorder.
        
        Args:
            base_dir: Base directory for recordings. If None, recording is disabled.
                     Creates a timestamped session subdirectory on initialization.
        """
        self.base_dir = base_dir
        self.sequence_counter = 0
        self.session_start_time = datetime.now()
        
        if self.base_dir:
            # Create base directory with session timestamp
            timestamp = self.session_start_time.strftime("%Y%m%d_%H%M%S")
            self.session_dir = os.path.join(self.base_dir, f"session_{timestamp}")
            os.makedirs(self.session_dir, exist_ok=True)
            
            # Create subdirectories
            self.agent_loop_dir = os.path.join(self.session_dir, "agent_loop")
            self.llm_resolution_dir = os.path.join(self.session_dir, "llm_resolution")
            os.makedirs(self.agent_loop_dir, exist_ok=True)
            os.makedirs(self.llm_resolution_dir, exist_ok=True)
        else:
            self.session_dir = None
            self.agent_loop_dir = None
            self.llm_resolution_dir = None
    
    def is_enabled(self) -> bool:
        """Check if recording is enabled."""
        return self.base_dir is not None
    
    def _get_next_sequence(self) -> int:
        """Get next sequence number."""
        self.sequence_counter += 1
        return self.sequence_counter
    
    def _get_timestamp(self) -> str:
        """Get current timestamp string."""
        return datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    
    def record_agent_loop_request(
        self,
        transaction_id: Optional[Any] = None,
        iteration: int = 1,
        request_body: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Record an agent loop request.
        
        Args:
            transaction_id: Transaction ID being processed
            iteration: Iteration number in the agent loop
            request_body: Request body sent to Bedrock
            
        Returns:
            Path to saved file, or None if recording disabled
        """
        if not self.is_enabled():
            return None
        
        seq = self._get_next_sequence()
        timestamp = self._get_timestamp()
        
        # Create filename
        txn_str = f"txn_{transaction_id}_" if transaction_id is not None else ""
        filename = f"{seq:04d}_{timestamp}_{txn_str}iteration_{iteration}_request.json"
        filepath = os.path.join(self.agent_loop_dir, filename)
        
        # Save request
        data = {
            "type": "agent_loop_request",
            "sequence": seq,
            "timestamp": timestamp,
            "transaction_id": str(transaction_id) if transaction_id is not None else None,
            "iteration": iteration,
            "request": request_body
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath
    
    def record_agent_loop_response(
        self,
        transaction_id: Optional[Any] = None,
        iteration: int = 1,
        response_body: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Record an agent loop response.
        
        Args:
            transaction_id: Transaction ID being processed
            iteration: Iteration number in the agent loop
            response_body: Response body received from Bedrock
            
        Returns:
            Path to saved file, or None if recording disabled
        """
        if not self.is_enabled():
            return None
        
        seq = self._get_next_sequence()
        timestamp = self._get_timestamp()
        
        # Create filename
        txn_str = f"txn_{transaction_id}_" if transaction_id is not None else ""
        filename = f"{seq:04d}_{timestamp}_{txn_str}iteration_{iteration}_response.json"
        filepath = os.path.join(self.agent_loop_dir, filename)
        
        # Save response
        data = {
            "type": "agent_loop_response",
            "sequence": seq,
            "timestamp": timestamp,
            "transaction_id": str(transaction_id) if transaction_id is not None else None,
            "iteration": iteration,
            "response": response_body
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath
    
    def record_llm_resolution_request(
        self,
        constraint: str,
        description: Optional[str] = None,
        row_data: Optional[Dict[str, Any]] = None,
        request_body: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Record an LLM resolution request.
        
        Args:
            constraint: Policy constraint that was violated
            description: Optional policy description
            row_data: Row data that violated the constraint
            request_body: Request body sent to Bedrock
            
        Returns:
            Path to saved file, or None if recording disabled
        """
        if not self.is_enabled():
            return None
        
        seq = self._get_next_sequence()
        timestamp = self._get_timestamp()
        
        # Create filename
        filename = f"{seq:04d}_{timestamp}_llm_resolution_request.json"
        filepath = os.path.join(self.llm_resolution_dir, filename)
        
        # Save request
        data = {
            "type": "llm_resolution_request",
            "sequence": seq,
            "timestamp": timestamp,
            "constraint": constraint,
            "description": description,
            "row_data": row_data,
            "request": request_body
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath
    
    def record_llm_resolution_response(
        self,
        constraint: str,
        description: Optional[str] = None,
        response_body: Optional[Dict[str, Any]] = None,
        fixed_row_data: Optional[Any] = None
    ) -> Optional[str]:
        """Record an LLM resolution response.
        
        Args:
            constraint: Policy constraint that was violated
            description: Optional policy description
            response_body: Response body received from Bedrock
            fixed_row_data: Fixed row data returned by LLM (if any)
            
        Returns:
            Path to saved file, or None if recording disabled
        """
        if not self.is_enabled():
            return None
        
        seq = self._get_next_sequence()
        timestamp = self._get_timestamp()
        
        # Create filename
        filename = f"{seq:04d}_{timestamp}_llm_resolution_response.json"
        filepath = os.path.join(self.llm_resolution_dir, filename)
        
        # Save response
        data = {
            "type": "llm_resolution_response",
            "sequence": seq,
            "timestamp": timestamp,
            "constraint": constraint,
            "description": description,
            "response": response_body,
            "fixed_row_data": fixed_row_data
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath
