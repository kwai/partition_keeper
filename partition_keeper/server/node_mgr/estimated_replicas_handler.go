package node_mgr

import "sync"

type EstimatedReplicaHandler struct {
	mu sync.RWMutex
	// node_id -> table_id -> estimated_replicas
	estReplicas map[string]map[int32]int
}

func NewEstimatedReplicasHandler() *EstimatedReplicaHandler {
	return &EstimatedReplicaHandler{
		mu:          sync.RWMutex{},
		estReplicas: make(map[string]map[int32]int),
	}
}

func (e *EstimatedReplicaHandler) Update(tableId int32, estCount map[string]int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if estCount == nil {
		estCount = map[string]int{}
	}

	for nodeId, count := range estCount {
		if nodeCount, ok := e.estReplicas[nodeId]; ok {
			nodeCount[tableId] = count
		} else {
			e.estReplicas[nodeId] = map[int32]int{tableId: count}
		}
	}

	drainedNodes := []string{}
	for nodeId, nodeCount := range e.estReplicas {
		if _, ok := estCount[nodeId]; !ok {
			delete(nodeCount, tableId)
		}
		if len(nodeCount) == 0 {
			drainedNodes = append(drainedNodes, nodeId)
		}
	}

	for _, node := range drainedNodes {
		delete(e.estReplicas, node)
	}
}

func (e *EstimatedReplicaHandler) Get(nodeId string, accessor func(nodeCount map[int32]int)) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if nodeCount, ok := e.estReplicas[nodeId]; ok {
		accessor(nodeCount)
	} else {
		accessor(nil)
	}
}
