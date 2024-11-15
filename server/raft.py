import grpc
import time
import random
import os
import json
import threading
import logging
from conts import FILE_STORAGE_DIR
from concurrent import futures
from typing import List
from lms_pb2 import (
    VoteRequest, VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    LogEntry, LeaderInfo, UploadFileAllResponse, UploadFileAllRequest
)

from lms_pb2_grpc import RaftServiceServicer, add_RaftServiceServicer_to_server, RaftServiceStub
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PEER_NODES = [
    "lms_server_1:5000",  # Example: First node
    "lms_server_2:5000",  # Example: Second node
    "lms_server_3:5000"   # Example: Third node
]
PEER_NODES.remove(f"{os.getenv('SERVER_NAME', None)}:5000")
class RaftNode(RaftServiceServicer):
    def __init__(self):
        self.role = "Follower"  # Role: Follower, Candidate, or Leader
        self.node_id = os.getenv('SERVER_NAME', None)
        self.node_address = f"{os.getenv('SERVER_NAME', None)}:5000"
        self.peers = PEER_NODES # List of peer addresses (host:port) of other nodes
        self.current_term = 0  # Current term of the node
        self.voted_for = None  # Node that this node voted for in the current term
        self.log_storage_path = "/app/logs/raft.log"
        self.log = self.load_log()  # Log entries for consistency
        self.commit_index = 0  # Index of the last committed log entry
        self.last_applied = 0  # Index of the last applied log entry
        logger.info(f"PEERS: - {os.getenv('SERVER_NAME', None)}:5000 =  {self.peers}")
        self.next_index = {peer: 0 for peer in self.peers}  # Next log index to send to each peer
        self.match_index = {peer: 0 for peer in self.peers}  # Highest log index known to be replicated on each peer
        self.votes_received = 0  # Votes received during election
        self.heartbeat_count = 0


        # Set up election timeout and heartbeat timer
        self.heartbeat_interval = 5  # Send heartbeats every second as leader
        self.election_timeout = self._random_timeout()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)

        # Start the election timer for the follower
        self.election_timer.start()
        logger.info(f"Node {self.node_id} initialized as Follower")
    
    def update_role(self, role: str):
        """Update the role of the node."""
        self.role = role
        os.environ['ROLE'] = role

    def is_leader(self) -> bool:
        """Check if the node is the leader."""
        return self.role == "Leader"

    def load_log(self) -> List[LogEntry]:
        """Load the log from disk."""
        if os.path.exists(self.log_storage_path):
            with open(self.log_storage_path, "r") as f:
                log_entries = []
                for line in f:
                    data = json.loads(line)
                    log_entry = LogEntry(term=data['term'], data=data['data'])  # Adjust if data type differs
                    log_entries.append(log_entry)
                return log_entries
        return []


    def log_entry_to_dict(self,log_entry: LogEntry) -> dict:
        return {
            'term': log_entry.term,
            'data': log_entry.data
        }

    def save_log(self):
        """Save the log to disk."""
        with open(self.log_storage_path, "w") as f:
            for entry in self.log:
                f.write(json.dumps(self.log_entry_to_dict(entry)) + "\n")

    def _random_timeout(self):
        """Generate a random election timeout to avoid split votes."""
        return random.uniform(self.heartbeat_interval + 1, 10)

    def start_election(self):
        """Start a new election if no leader heartbeat received."""
        self.update_role("Candidate")
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        logger.info("----------------------Election Started----------------------")
        logger.info(f"[{self.role}] Node {self.node_id} started an election for term {self.current_term}")

        # Send RequestVote RPCs to all peers
        for peer in self.peers:
            threading.Thread(target=self.request_vote, args=(peer,)).start()

    def request_vote(self, peer: str):
        """Send RequestVote RPC to a peer."""
        stub = self._get_stub(peer)
        request = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=len(self.log) - 1,
            last_log_term=self.log[-1].term if self.log else 0
        )
        try:
            response = stub.RequestVote(request)
            self.handle_vote_response(response)
        except grpc.RpcError as e:
            logger.info(f"[{self.role}] Failed to request vote from {peer}: Server did not respond")

    def handle_vote_response(self, response: VoteResponse):
        """Handle the response of a vote request."""
        if response.vote_granted:
            self.votes_received += 1
            logger.info(f"[{self.role}] Node {self.node_id} received a vote. Total votes: {self.votes_received}")

            if self.votes_received > len(self.peers) // 2:
                self.become_leader()
        logger.info("----------------------Election Ended----------------------")

    def become_leader(self):
        """Become the leader and start sending heartbeats."""
        self.update_role("Leader")
        logger.info(f"[{self.role}] Node {self.node_id} became the Leader for term {self.current_term}")

        # Initialize nextIndex for all peers
        for peer in self.peers:
            self.next_index[peer] = len(self.log)

        # Start sending heartbeats
        threading.Thread(target=self.send_heartbeats).start()

    def send_heartbeats(self):
        """Send periodic heartbeats to followers."""
        while self.role == "Leader":
            for peer in self.peers:
                entries = []  # Heartbeats do not contain entries
                threading.Thread(target=self.append_entries, args=(peer, entries)).start()
            time.sleep(self.heartbeat_interval)

    def append_entries(self, peer: str, entries: List[LogEntry]):
        """Send AppendEntries RPC to a peer based on proto definition."""
        stub = self._get_stub(peer)

        # Determine the prev_log_index and prev_log_term to send in the request
        prev_log_index = self.next_index[peer] - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

        request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            commit_index=self.commit_index
        )
        try:
            if len(entries) == 0:
                logger.info(f"[{self.role}] Sending heartbeat {self.heartbeat_count} to {peer}")
                self.heartbeat_count += 1
            # Send AppendEntries RPC to the follower
            response = stub.AppendEntries(request)
            if response.success:
                # Log replicated successfully, update next_index and match_index
                self.match_index[peer] = prev_log_index + len(entries)
                self.next_index[peer] = self.match_index[peer] + 1
                # logger.info(f"[{self.role}] AppendEntries successful for {peer}. Updated next_index to {self.next_index[peer]}. {response.success}")
            else:
                # Log mismatch, decrement next_index and retry
                self.next_index[peer] = max(0, self.next_index[peer] - 1)
                logger.warning(f"AppendEntries failed for {peer}, retrying with next_index={self.next_index[peer]}")
                
                # Retry sending entries if there's still more log to send
                if self.next_index[peer] >= 0:
                    self.append_entries(peer, self.log[self.next_index[peer]:])

            # Handle cases where the follower has a higher term (leader step down)
            if response.term > self.current_term:
                logger.info(f"[{self.role}] Term out of date. Stepping down. Peer term: {response.term}, current term: {self.current_term}")
                self.current_term = response.term
                self.update_role("Follower")
                self.save_term()

            return response

        except grpc.RpcError as e:
            logger.error(f"Failed to append entries to {peer}: Either server is down or there is an error ")
            return None

    def handle_append_response(self, response: AppendEntriesResponse):
        """Handle the response of an AppendEntries RPC."""
        if response.success:
            logger.info(f"[{self.role}] Log entry replicated on node {response.node_id}")
        else:
            logger.info(f"[{self.role}] AppendEntries failed on node {response.node_id}. Retrying...")

    def _get_stub(self, peer: str):
        """Get the gRPC stub for a peer."""
        channel = grpc.insecure_channel(peer)
        return RaftServiceStub(channel)

    def propose_log_entry(self, data) -> bool:
        """Propose a new log entry to be committed by the leader."""
        logger.info("----------------------Propose Log Entry----------------------")
        if self.role != "Leader":
            logger.info(f"[{self.role}] Node {self.node_id} is not the leader and cannot propose log entry.")
            return False

        # Create a new log entry with the current term and data
        new_log_entry = LogEntry(term=self.current_term, data=data)
        self.log.append(new_log_entry)
        self.save_log()  # Persist the updated log to disk or storage

        # Send AppendEntries RPC to all peers
        votes_received = 1  # Start with the leader's own vote

        for peer in self.peers:  # Assuming peers is a list of peer addresses (host:port)
            if peer not in self.next_index:
                logger.error(f"Peer {peer} has no next_index entry.")
                continue

            # If the peer's log is shorter, send all missing entries
            if self.next_index[peer] < len(self.log):
                # Get the missing log entries for this peer
                missing_entries = self.log[self.next_index[peer]:]  # Fetch missing entries starting from next_index
                response = self.append_entries(peer, missing_entries)  # Send AppendEntries with missing entries
            else:
                # Send only the new log entry if logs are consistent
                response = self.append_entries(peer, [new_log_entry])

            # Check if response is None or has a success attribute
            if response is None:
                logger.error(f"No response received from peer {peer}.")
                continue  # Skip processing for this peer if no response

            # Process the response
            if response.success:
                votes_received += 1
            elif response.term > self.current_term:
                # If the peer's term is higher, the current leader must step down
                logger.info(f"[{self.role}] Term out of date. Stepping down. Peer term: {response.term}, current term: {self.current_term}")
                self.current_term = response.term
                self.update_role("Follower")
                self.save_term()
                return False

        # If a majority of votes are received, commit the log entry
        if votes_received > len(self.peers) // 2:
            self.commit_index += 1  # Increment commit index
            self.save_log()  # Persist the log after committing
            logger.info(f"[{self.role}] Log entry committed by majority. Data: {data}")
            return True
        else:
            logger.info(f"[{self.role}] Not enough votes to commit log entry. Votes received: {votes_received}")
            return False
        logger.info("----------------------Propose Log Entry Concluded ----------------------")

    # RPC handlers for Raft protocol
    def RequestVote(self, request, context):
        """Handle RequestVote RPC from a candidate."""
        if (request.term > self.current_term or
                (request.term == self.current_term and self.voted_for in (None, request.candidate_id))):
            self.voted_for = request.candidate_id
            self.current_term = request.term
            self.election_timer.cancel()  # Reset election timer
            return VoteResponse(term=self.current_term, vote_granted=True)
        return VoteResponse(term=self.current_term, vote_granted=False)

    def AppendEntries(self, request: AppendEntriesRequest, context):
        """Follower handling of AppendEntries RPC based on proto definition."""

        # Reject if the leader's term is outdated
        if request.term < self.current_term:
            logger.warning(f"Received outdated term: {request.term}. Current term: {self.current_term}.")
            return AppendEntriesResponse(term=self.current_term, success=False, node_id=self.node_id)

        # If the leader's term is higher, update the current term and become a follower
        if request.term > self.current_term:
            logger.info(f"[{self.role}] Term updated: {self.current_term} -> {request.term}. Becoming follower.")
            self.current_term = request.term
            self.role = 'follower'
            self.voted_for = None
            # self.save_state() // Review later

        # Reset the election timer
        self.election_timer.cancel()
        self.election_timer = threading.Timer(self._random_timeout(), self.start_election)
        self.election_timer.start()

        # Check log consistency with prev_log_index and prev_log_term
        if request.prev_log_index >= 0:
            if len(self.log) <= request.prev_log_index:
                # Log is too short, reject the request
                logger.warning(f"Log consistency failed: Follower's log too short (length {len(self.log)}).")
                return AppendEntriesResponse(term=self.current_term, success=False, node_id=self.node_id)

            if self.log[request.prev_log_index].term != request.prev_log_term:
                # Log term mismatch at prev_log_index
                logger.warning(f"Log consistency failed: Term mismatch at index {request.prev_log_index}.")
                # Optional: Truncate log if necessary (depending on your protocol logic)
                self.log = self.log[:request.prev_log_index]
                self.save_log()  # Persist the truncated log
                return AppendEntriesResponse(term=self.current_term, success=False, node_id=self.node_id)

        # Append new log entries (if any) after prev_log_index
        new_entries = [LogEntry(term=entry.term, data=entry.data) for entry in request.entries]  # Convert to list of LogEntry
        if new_entries:
            logger.info(f"[{self.role}] Appending {len(new_entries)} new entries to the log.")
            # Remove conflicting entries and append new ones
            self.log = self.log[:request.prev_log_index + 1] + new_entries
            self.save_log()  # Persist the updated log

        # Update commit index and apply new entries to the state machine
        if request.commit_index > self.commit_index:
            # Update commit index only if it's within the bounds of the log
            if request.commit_index < len(self.log):
                self.commit_index = request.commit_index
            else:
                self.commit_index = len(self.log) - 1  # Adjust to the last log index

        # Return success after log has been updated
        # logger.info(f"[{self.role}] AppendEntries succeeded, sending success response.")
        return AppendEntriesResponse(term=self.current_term, success=True, node_id=self.node_id)

    def GetLeader(self, request, context):
        """Handle GetLeader RPC."""
        if self.role == "Leader":
            leader_address = self.node_address
        else:
            leader_address = ""

        return LeaderInfo(leader_address=leader_address)
    
    def upload_to_all_nodes(self, file_name, file_content):
        for peer in self.peers:
            stub = self._get_stub(peer)
            try:
                request = UploadFileAllRequest(filename=file_name, data=file_content)
                response = stub.UploadFileAll(request)
            except grpc.RpcError as e:
                logger.info(f"[{self.role}] Failed to upload file to {peer}: Server did not respond")
    
    def UploadFileAll(self, request, context):
        file_name = request.filename
        file_content = request.data
        file_path = os.path.join(FILE_STORAGE_DIR, file_name)
        with open(file_path, 'wb') as f:
            f.write(file_content)
        return UploadFileAllResponse(status="success")

# To run the server, create a function similar to the following:
def serve(peers: List[str]):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(peers)
    add_RaftServiceServicer_to_server(raft_node, server)
    server.add_insecure_port('[::]:50051')  # Bind to port (can be adjusted)
    server.start()
    logger.info("Raft server started...")
    server.wait_for_termination()

raft_service = RaftNode()