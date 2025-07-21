# Federated Learning Implementation Design for TrustMesh

## Overview

This document outlines the careful modifications needed to enable true multi-node federated learning in TrustMesh while maintaining compatibility with existing Redis cluster, CouchDB, smart contracts, and task executor logic.

## Core Architecture Challenges

1. **Current State**: Each IoT node creates a unique `schedule_id`, resulting in separate workflow executions
2. **Required State**: Multiple IoT nodes must contribute to the same `schedule_id` for federated aggregation
3. **Privacy Constraint**: Nodes must not share raw data, only model parameters

## Proposed Architecture Modifications

### 1. Transaction Initiator Modifications

**File**: `iot-node/transaction_initiator/transaction_initiator.py`

**Changes Required**:
- Add support for "coordinator" and "participant" modes
- Coordinator creates new `schedule_id`, participants use existing one
- Maintain backward compatibility for non-federated workflows

```python
def create_and_send_transactions(self, iot_data, workflow_id, iot_port, iot_public_key, 
                                schedule_id=None, is_coordinator=False):
    if schedule_id is None and not is_coordinator:
        # Traditional single-node mode
        schedule_id = str(uuid.uuid4())
        create_schedule = True
    elif is_coordinator:
        # Federated coordinator mode
        schedule_id = str(uuid.uuid4())
        create_schedule = True
    else:
        # Federated participant mode
        create_schedule = False
```

### 2. IoT Data Transaction Processor Modifications

**File**: `scheduling/iot-data-tp/iot_data_tp.py`

**Critical Issue**: Current implementation uses `redis.set()` which overwrites data

**Required Changes**:
- Implement data accumulation for federated workflows
- Use Redis lists or sets to store multiple node submissions
- Maintain atomicity and consistency

```python
# New storage pattern for federated data
def store_federated_data(self, workflow_id, schedule_id, app_id, node_id, data):
    # Use Redis list to accumulate data from multiple nodes
    key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_federated"
    node_data = {
        "node_id": node_id,
        "data": data,
        "timestamp": time.time()
    }
    await self.redis.rpush(key, json.dumps(node_data))
```

### 3. Scheduling Modifications

**File**: `compute-node/event_handlers/schedule_event_handler.py`

**Required Changes**:
- Add logic to detect when all expected nodes have submitted data
- Implement timeout mechanisms for missing nodes
- Trigger scheduling only when sufficient nodes participate

### 4. Task Executor Modifications

**File**: `compute-node/task_executor/task_executor.py`

**Required Changes**:
- Modify `fetch_dependency_outputs` to handle federated data structure
- Aggregate data from multiple nodes before passing to tasks

## Federated Learning Workflow Structure

### Workflow Definition
```json
{
    "workflow_id": "federated-mnist-learning",
    "expected_nodes": 5,
    "federation_config": {
        "coordinator_node": "iot-0",
        "participant_nodes": ["iot-1", "iot-2", "iot-3", "iot-4"],
        "min_nodes_required": 3,
        "round_timeout": 300
    }
}
```

### Data Flow

1. **Round Initialization**:
   - Coordinator (iot-0) creates schedule with federation metadata
   - Broadcasts schedule_id to participant nodes

2. **Data Submission**:
   - Each node submits local training data to shared schedule_id
   - iot-data-tp accumulates submissions in Redis list

3. **Scheduling Trigger**:
   - Monitor for minimum node participation
   - Timeout after round_timeout seconds
   - Create computation schedule when conditions met

4. **Task Execution**:
   - Local training tasks process individual node data
   - Aggregation task collects all model updates
   - Evaluation task assesses global model

## Redis Data Structures

### Federated Data Storage
```
Key: iot_data_{workflow_id}_{schedule_id}_{app_id}_federated
Type: LIST
Value: [
    {"node_id": "iot-0", "data": {...}, "timestamp": ...},
    {"node_id": "iot-1", "data": {...}, "timestamp": ...},
    ...
]
```

### Federation State Tracking
```
Key: federation_state_{workflow_id}_{schedule_id}
Type: HASH
Fields:
    - expected_nodes: 5
    - submitted_nodes: ["iot-0", "iot-1", ...]
    - round_start_time: timestamp
    - status: "collecting" | "ready" | "timeout"
```

## CouchDB Considerations

- Store federated workflow metadata in CouchDB
- Track historical rounds and performance metrics
- Maintain node participation records

## Smart Contract Implications

### New State Variables
- Federation workflow registry
- Node participation tracking
- Round completion verification

### Transaction Validation
- Verify node is authorized for federation
- Prevent duplicate submissions from same node
- Enforce minimum participation requirements

## Backward Compatibility

- Non-federated workflows continue using existing flow
- Federation mode activated only with explicit configuration
- Graceful fallback for incomplete rounds

## Implementation Phases

### Phase 1: Core Infrastructure
1. Modify transaction_initiator for coordinator/participant modes
2. Update iot-data-tp for data accumulation
3. Add federation state tracking

### Phase 2: Scheduling Integration
1. Enhance schedule_event_handler for federation awareness
2. Implement participation monitoring
3. Add timeout mechanisms

### Phase 3: Task Execution
1. Update task_executor for federated data handling
2. Implement model aggregation logic
3. Add evaluation and distribution tasks

### Phase 4: Testing & Validation
1. Test with varying node participation
2. Verify data privacy preservation
3. Validate fault tolerance scenarios

## Risk Mitigation

1. **Race Conditions**: Use Redis transactions for atomic operations
2. **Node Failures**: Implement minimum participation thresholds
3. **Data Consistency**: Add checksums and verification
4. **Performance**: Optimize Redis operations for large model parameters

## Next Steps

1. Review and approve design
2. Implement Phase 1 modifications
3. Create test scenarios
4. Iterative implementation of remaining phases