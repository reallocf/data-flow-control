"""
Replay utility for loading and serving recorded LLM responses.

Allows replaying a previous session by matching incoming requests
to recorded responses and returning them instead of calling the LLM.

The replay manager:
- Loads all recorded requests and responses from a session directory
- Matches incoming requests to recorded requests:
  - Agent loop: by transaction ID + iteration number
  - LLM resolution: by constraint + description + row_data
- Returns recorded responses when matches are found
- Applies optional delay before returning responses (to simulate network latency)
- Falls back to sequential replay if exact match fails
- Falls back to actual LLM call if no recorded response is available

Usage:
    from replay import ReplayManager
    
    replay_manager = ReplayManager(session_dir="session_records/session_20260117_100205", delay_ms=500)
    if replay_manager.is_enabled():
        response = replay_manager.get_agent_loop_response(
            transaction_id=1,
            iteration=1,
            request_body={...}
        )
"""

import os
import json
import time
from typing import Optional, Dict, Any, List
from pathlib import Path


class ReplayManager:
    """Manages replaying recorded LLM responses from a session directory.
    
    Loads all recorded requests and responses from a session directory and provides
    methods to retrieve recorded responses for incoming requests. Uses intelligent
    matching to find the correct response for each request, with fallback mechanisms
    for robustness. Can apply optional delays before returning responses to simulate
    network latency for demos.
    
    Example:
        replay_manager = ReplayManager(session_dir="session_records/session_20260117_100205", delay_ms=500)
        # Loads all files from:
        #   session_records/session_20260117_100205/agent_loop/
        #   session_records/session_20260117_100205/llm_resolution/
        # Applies 500ms delay before returning responses
    """
    
    def __init__(self, session_dir: str, delay_ms: int = 0):
        """Initialize the replay manager.
        
        Args:
            session_dir: Path to the session recording directory (e.g., 
                        "session_records/session_20260117_100205")
            delay_ms: Optional delay in milliseconds to apply before returning responses.
                     Useful for demos to simulate network latency. Default: 0 (no delay).
        """
        self.session_dir = session_dir
        self.delay_ms = delay_ms
        self.agent_loop_dir = os.path.join(session_dir, "agent_loop")
        self.llm_resolution_dir = os.path.join(session_dir, "llm_resolution")
        
        # Load all recorded files
        self.agent_loop_requests: List[Dict[str, Any]] = []
        self.agent_loop_responses: List[Dict[str, Any]] = []
        self.llm_resolution_requests: List[Dict[str, Any]] = []
        self.llm_resolution_responses: List[Dict[str, Any]] = []
        
        # Index for quick lookup
        self.agent_loop_index: Dict[str, Dict[str, Any]] = {}
        self.llm_resolution_index: Dict[str, Dict[str, Any]] = {}
        
        # Counters for sequential replay
        self.agent_loop_counter = 0
        self.llm_resolution_counter = 0
        
        self._load_recordings()
    
    def _load_recordings(self):
        """Load all recorded files from the session directory."""
        # Load agent loop files
        if os.path.exists(self.agent_loop_dir):
            for filename in sorted(os.listdir(self.agent_loop_dir)):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.agent_loop_dir, filename)
                    try:
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                            if data.get('type') == 'agent_loop_request':
                                self.agent_loop_requests.append(data)
                            elif data.get('type') == 'agent_loop_response':
                                self.agent_loop_responses.append(data)
                    except Exception as e:
                        print(f"[WARNING] Failed to load {filepath}: {e}")
        
        # Load LLM resolution files
        if os.path.exists(self.llm_resolution_dir):
            for filename in sorted(os.listdir(self.llm_resolution_dir)):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.llm_resolution_dir, filename)
                    try:
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                            if data.get('type') == 'llm_resolution_request':
                                self.llm_resolution_requests.append(data)
                            elif data.get('type') == 'llm_resolution_response':
                                self.llm_resolution_responses.append(data)
                    except Exception as e:
                        print(f"[WARNING] Failed to load {filepath}: {e}")
        
        # Build index for agent loop: key = (transaction_id, iteration)
        for req in self.agent_loop_requests:
            txn_id = str(req.get('transaction_id', ''))
            iteration = req.get('iteration', 0)
            key = f"{txn_id}_{iteration}"
            self.agent_loop_index[key] = req
        
        # Match responses to requests by sequence number and transaction/iteration
        # Responses follow requests, so we can match them sequentially
        agent_responses_by_seq = {r.get('sequence'): r for r in self.agent_loop_responses}
        for req in self.agent_loop_requests:
            req_seq = req.get('sequence', 0)
            req_txn_id = str(req.get('transaction_id', ''))
            req_iteration = req.get('iteration', 0)
            
            # Find the next response after this request that matches transaction and iteration
            matching_resp = None
            for resp_seq in sorted(agent_responses_by_seq.keys()):
                if resp_seq > req_seq:
                    resp = agent_responses_by_seq[resp_seq]
                    resp_txn_id = str(resp.get('transaction_id', ''))
                    resp_iteration = resp.get('iteration', 0)
                    if resp_txn_id == req_txn_id and resp_iteration == req_iteration:
                        matching_resp = resp
                        break
            
            if matching_resp:
                key = f"{req_txn_id}_{req_iteration}"
                if key not in self.agent_loop_index:
                    self.agent_loop_index[key] = {}
                self.agent_loop_index[key]['response'] = matching_resp
        
        # Build index for LLM resolution: key = (constraint, description, row_data_hash)
        for req in self.llm_resolution_requests:
            constraint = req.get('constraint', '')
            description = req.get('description', '')
            row_data = req.get('row_data', {})
            # Create a hash of row_data for matching
            row_data_str = json.dumps(row_data, sort_keys=True)
            key = f"{constraint}|{description}|{row_data_str}"
            self.llm_resolution_index[key] = req
        
        # Match responses to requests by sequence number and constraint/description/row_data
        llm_responses_by_seq = {r.get('sequence'): r for r in self.llm_resolution_responses}
        for req in self.llm_resolution_requests:
            req_seq = req.get('sequence', 0)
            req_constraint = req.get('constraint', '')
            req_description = req.get('description', '')
            req_row_data = req.get('row_data', {})
            
            # Find the next response after this request that matches constraint/description
            matching_resp = None
            for resp_seq in sorted(llm_responses_by_seq.keys()):
                if resp_seq > req_seq:
                    resp = llm_responses_by_seq[resp_seq]
                    resp_constraint = resp.get('constraint', '')
                    resp_description = resp.get('description', '')
                    if resp_constraint == req_constraint and resp_description == req_description:
                        matching_resp = resp
                        break
            
            if matching_resp:
                row_data_str = json.dumps(req_row_data, sort_keys=True)
                key = f"{req_constraint}|{req_description}|{row_data_str}"
                if key not in self.llm_resolution_index:
                    self.llm_resolution_index[key] = {}
                self.llm_resolution_index[key]['response'] = matching_resp
    
    def is_enabled(self) -> bool:
        """Check if replay is enabled."""
        return self.session_dir is not None and os.path.exists(self.session_dir)
    
    def get_agent_loop_response(
        self,
        transaction_id: Optional[Any],
        iteration: int,
        request_body: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get recorded agent loop response for a request.
        
        Args:
            transaction_id: Transaction ID being processed
            iteration: Iteration number in the agent loop
            request_body: Request body sent to Bedrock
            
        Returns:
            Recorded response body, or None if not found
            
        Note:
            If delay_ms > 0, applies a delay before returning the response to simulate
            network latency. This only applies when replaying (not for actual LLM calls).
        """
        if not self.is_enabled():
            return None
        
        txn_id_str = str(transaction_id) if transaction_id is not None else ''
        key = f"{txn_id_str}_{iteration}"
        
        response_body = None
        
        if key in self.agent_loop_index:
            entry = self.agent_loop_index[key]
            if 'response' in entry:
                response_body = entry['response'].get('response')
        
        # Fallback: sequential replay
        if response_body is None and self.agent_loop_counter < len(self.agent_loop_responses):
            response_data = self.agent_loop_responses[self.agent_loop_counter]
            self.agent_loop_counter += 1
            response_body = response_data.get('response')
        
        # Apply delay if configured and response found
        if response_body is not None and self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)
        
        return response_body
    
    def get_llm_resolution_response(
        self,
        constraint: str,
        description: Optional[str],
        row_data: Dict[str, Any],
        request_body: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get recorded LLM resolution response for a request.
        
        Args:
            constraint: Policy constraint that was violated
            description: Optional policy description
            row_data: Row data that violated the constraint
            request_body: Request body sent to Bedrock
            
        Returns:
            Recorded response body, or None if not found
            
        Note:
            If delay_ms > 0, applies a delay before returning the response to simulate
            network latency. This only applies when replaying (not for actual LLM calls).
        """
        if not self.is_enabled():
            return None
        
        desc = description or ''
        row_data_str = json.dumps(row_data, sort_keys=True)
        key = f"{constraint}|{desc}|{row_data_str}"
        
        response_body = None
        
        if key in self.llm_resolution_index:
            entry = self.llm_resolution_index[key]
            if 'response' in entry:
                response_body = entry['response'].get('response')
        
        # Fallback: sequential replay
        if response_body is None and self.llm_resolution_counter < len(self.llm_resolution_responses):
            response_data = self.llm_resolution_responses[self.llm_resolution_counter]
            self.llm_resolution_counter += 1
            response_body = response_data.get('response')
        
        # Apply delay if configured and response found
        if response_body is not None and self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)
        
        return response_body
