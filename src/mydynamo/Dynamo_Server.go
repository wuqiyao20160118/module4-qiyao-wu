package mydynamo

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	isAlive        bool         //false means a crash state
	store          map[string][]ObjectEntry
	futureGossip   []map[string][]ObjectEntry // key: key of PutArgs  value: PutArgs
	mu             sync.Mutex
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	s.futureGossip = make([]map[string][]ObjectEntry, len(s.preferenceList))
	for i := 0; i < len(s.futureGossip); i++ {
		s.futureGossip[i] = make(map[string][]ObjectEntry)
	}
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	if !s.isAlive {
		return errors.New("server has crashed")
	}

	for i := 1; i < len(s.preferenceList); i++ {
		log.Println("---------------------------")
		log.Printf("Preparing to establish RPC Connection to %s", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		rpcConn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		if e != nil {
			continue
		}

		s.mu.Lock()

		gossipMap := s.futureGossip[i]
		toDeleteKey := make([]string, 0)
		for key, entryList := range gossipMap {
			batchArgs := BatchReplicateArgs{
				EntryList: entryList,
				Key:       key,
			}
			res := true
			// res return false if the remote node crashes currently
			log.Printf("RPC to %s Connected! Preparing to call MyDynamo.Replicate for key: %s...\n", s.preferenceList[i].Address+":"+s.preferenceList[i].Port, key)
			e = rpcConn.Call("MyDynamo.BatchReplicate", batchArgs, &res)
			if e == nil {
				toDeleteKey = append(toDeleteKey, key)
			}
		}

		if rpcConn != nil {
			e = rpcConn.Close()
			if e != nil {
				fmt.Println("CleanConnError", e)
			}
		}

		// delete the successfully gossip key/value pairs
		for _, key := range toDeleteKey {
			delete(gossipMap, key)
		}

		s.mu.Unlock()
	}

	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	if !s.isAlive {
		*success = false
		log.Println("server has crashed")
		return nil
	}
	*success = true

	go func(sec int) {
		s.mu.Lock()
		s.isAlive = false
		s.mu.Unlock()
		time.Sleep(time.Duration(sec) * time.Second)
		s.mu.Lock()
		s.isAlive = true
		s.mu.Unlock()
	}(seconds)
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	if !s.isAlive {
		*result = false
		return errors.New("server has crashed")
	}
	log.Println("---------------Put RPC received!---------------")

	var votes int = 1
	forwardValue := value

	success := true
	ctxVectorClock := value.Context
	key := value.Key
	val := value.Value
	// First attempt to put the value into its local key/value store
	s.mu.Lock()
	if kvStore, ok := s.store[key]; !ok {
		ctxVectorClock.Clock.Increment(s.nodeID)
		initialList := make([]ObjectEntry, 0)
		s.store[key] = append(initialList, ObjectEntry{
			Context: ctxVectorClock,
			Value:   val,
		})
		forwardValue.Context = ctxVectorClock
	} else {
		removeIndex := make([]int, 0)
		for i := 0; i < len(kvStore); i++ {
			if kvStore[i].Context.Clock.LessThan(ctxVectorClock.Clock) || kvStore[i].Context.Clock.Equals(ctxVectorClock.Clock) {
				removeIndex = append(removeIndex, i)
			} else if ctxVectorClock.Clock.LessThan(kvStore[i].Context.Clock) {
				success = false
				break
			}
		}

		if success {
			length := len(removeIndex)
			newEntryList := kvStore
			for i := 0; i < length; i++ {
				newEntryList = remove(kvStore, removeIndex[i]-i)
			}

			ctxVectorClock.Clock.Increment(s.nodeID)
			newEntryList = append(newEntryList, ObjectEntry{
				Context: ctxVectorClock,
				Value:   val,
			})
			s.store[key] = newEntryList
			forwardValue.Context = ctxVectorClock
		}
	}
	s.mu.Unlock()

	// Attempt to replicate that value to W-1 other nodes
	// Maybe need set up goroutines here!
	*result = false
	gossipStoreIdx := make([]int, 0)
	log.Printf("%d nodes in the preferenceList.\n", len(s.preferenceList))
	for i := 0; i < len(s.preferenceList); i++ {
		if i == 0 {
			log.Println("---------------------------")
			log.Println("Preparing to self-checking votes...")
			if votes >= s.wValue {
				for j := i + 1; j < len(s.preferenceList); j++ {
					gossipStoreIdx = append(gossipStoreIdx, j)
				}
				log.Println("Votes are over nWrite!")
				break
			}
			continue
		}
		log.Println("---------------------------")
		log.Printf("Preparing to establish RPC Connection to %s", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		rpcConn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		if e != nil {
			continue
		}
		log.Printf("RPC to %s Connected! Preparing to call MyDynamo.Replicate...\n", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)

		res := true
		e = rpcConn.Call("MyDynamo.Replicate", forwardValue, &res)
		if e == nil && res {
			log.Println("Replicate done!")
			votes += 1
			if votes >= s.wValue {
				*result = true
				for j := i + 1; j < len(s.preferenceList); j++ {
					gossipStoreIdx = append(gossipStoreIdx, j)
				}
				if rpcConn != nil {
					e = rpcConn.Close()
					if e != nil {
						fmt.Println("CleanConnError", e)
					}
					log.Println("RPC Closed because votes are over nWrite!")
				}
				break
			}
		} else {
			gossipStoreIdx = append(gossipStoreIdx, i)
		}

		if rpcConn != nil {
			e = rpcConn.Close()
			if e != nil {
				fmt.Println("CleanConnError", e)
			}
			log.Println("RPC Closed!")
		}
	}

	log.Println("----------------------------------")
	log.Println("Storing gossip map...")
	// all nodes that weren't replicated to should be stored
	// so that a future Gossip operation knows which nodes still need a copy of this data
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("The length of the gossipStoreIdx for %s is %d.\n", s.selfNode.Address+":"+s.selfNode.Port, len(gossipStoreIdx))
	for _, idx := range gossipStoreIdx {
		log.Printf("Storing idx: %d\n", idx)
		gossipMap := s.futureGossip[idx]
		putList, ok := gossipMap[forwardValue.Key]
		if !ok {
			arg := make([]ObjectEntry, 0)
			arg = append(arg, ObjectEntry{
				Context: forwardValue.Context,
				Value:   forwardValue.Value,
			})
			gossipMap[forwardValue.Key] = arg
		} else {
			// update new data to be replicated (GC for old version)
			suc := true
			removeIndex := make([]int, 0)
			for idx, putArg := range putList {
				if putArg.Context.Clock.LessThan(forwardValue.Context.Clock) || putArg.Context.Clock.Equals(forwardValue.Context.Clock) {
					removeIndex = append(removeIndex, idx)
				} else if forwardValue.Context.Clock.LessThan(putArg.Context.Clock) {
					suc = false
					break
				}
			}

			if suc {
				length := len(removeIndex)
				newList := putList
				for j := 0; j < length; j++ {
					newList = remove(putList, removeIndex[j]-j)
				}
				newList = append(newList, ObjectEntry{
					Context: forwardValue.Context,
					Value:   val,
				})
				gossipMap[forwardValue.Key] = newList
			}
		}
		log.Printf("Storing idx: %d done.\n", idx)
	}

	return nil
}

//Replicate the value to other nodes
func (s *DynamoServer) Replicate(value PutArgs, result *bool) error {
	if !s.isAlive {
		*result = false
		return errors.New("server has crashed")
	}

	success := true
	ctxVectorClock := value.Context
	key := value.Key
	val := value.Value

	s.mu.Lock()
	if kvStore, ok := s.store[key]; !ok {
		initialList := make([]ObjectEntry, 0)
		s.store[key] = append(initialList, ObjectEntry{
			Context: ctxVectorClock,
			Value:   val,
		})
		*result = true
	} else {
		removeIndex := make([]int, 0)
		for i := 0; i < len(kvStore); i++ {
			if kvStore[i].Context.Clock.LessThan(ctxVectorClock.Clock) {
				removeIndex = append(removeIndex, i)
			} else if ctxVectorClock.Clock.LessThan(kvStore[i].Context.Clock) {
				success = false
				*result = false
				break
			}
		}

		if success {
			length := len(removeIndex)
			newEntryList := kvStore
			for i := 0; i < length; i++ {
				newEntryList = remove(kvStore, removeIndex[i]-i)
			}

			newEntryList = append(newEntryList, ObjectEntry{
				Context: ctxVectorClock,
				Value:   val,
			})
			s.store[key] = newEntryList
			*result = true
		}
	}
	s.mu.Unlock()

	return nil
}

//Batch replicate a list of key/value pair to other nodes
func (s *DynamoServer) BatchReplicate(value BatchReplicateArgs, result *bool) error {
	if !s.isAlive {
		*result = false
		return errors.New("server has crashed")
	}

	log.Printf("Batch Replicate Recieved! Port: %s\n", s.selfNode.Address+":"+s.selfNode.Port)

	*result = true
	key := value.Key

	s.mu.Lock()
	if _, ok := s.store[key]; !ok {
		s.store[key] = value.EntryList
	} else {
		kvStore := s.store[key]
		entryList := value.EntryList
		for i := 0; i < len(entryList); i++ {
			success := true
			ctxVectorClock := entryList[i].Context
			val := entryList[i].Value

			removeIndex := make([]int, 0)
			for j := 0; j < len(kvStore); j++ {
				if kvStore[j].Context.Clock.LessThan(ctxVectorClock.Clock) {
					removeIndex = append(removeIndex, j)
				} else if ctxVectorClock.Clock.LessThan(kvStore[j].Context.Clock) || ctxVectorClock.Clock.Equals(kvStore[j].Context.Clock) {
					success = false
					break
				}
			}

			if success {
				length := len(removeIndex)
				newEntryList := kvStore
				for j := 0; j < length; j++ {
					newEntryList = remove(kvStore, removeIndex[j]-j)
				}

				newEntryList = append(newEntryList, ObjectEntry{
					Context: ctxVectorClock,
					Value:   val,
				})
				s.store[key] = newEntryList
			}
		}
	}

	log.Printf("Gossip on %s done!\n", s.selfNode.Address+":"+s.selfNode.Port)
	s.mu.Unlock()
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if !s.isAlive {
		return errors.New("server has crashed")
	}

	allReads := make([]ObjectEntry, 0)
	// First attempt to read the value from its local key/value store
	s.mu.Lock()
	if _, ok := s.store[key]; ok {
		result.EntryList = append(result.EntryList, s.store[key]...)
	}
	s.mu.Unlock()

	// Attempt to read that value from R-1 other nodes
	for i := 1; i < s.rValue; i++ {
		rpcConn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		if e != nil {
			continue
		}

		res := DynamoResult{
			EntryList: make([]ObjectEntry, 0),
		}
		e = rpcConn.Call("MyDynamo.NodeGet", key, &res)
		if e != nil {
			if rpcConn != nil {
				e = rpcConn.Close()
				if e != nil {
					fmt.Println("CleanConnError", e)
				}
			}
			continue
		}

		allReads = append(allReads, res.EntryList...)
		if rpcConn != nil {
			e = rpcConn.Close()
			if e != nil {
				fmt.Println("CleanConnError", e)
			}
		}
	}
	//s.mu.Unlock()

	// Combine all (context, value) pairs
	for _, entry := range allReads {
		removeIndex := make([]int, 0)
		needRemove := true
		for idx, preEntry := range result.EntryList {
			if preEntry.Context.Clock.LessThan(entry.Context.Clock) {
				removeIndex = append(removeIndex, idx)
			} else if entry.Context.Clock.LessThan(preEntry.Context.Clock) || entry.Context.Clock.Equals(preEntry.Context.Clock) {
				needRemove = false
				break
			}
		}
		if needRemove {
			// Remove + Append
			length := len(removeIndex)
			newEntry := result.EntryList
			for i := 0; i < length; i++ {
				newEntry = remove(result.EntryList, removeIndex[i]-i)
			}
			result.EntryList = append(newEntry, entry)
		}
	}

	sort.Sort(result)
	return nil
}

//Read issued by a node
func (s *DynamoServer) NodeGet(key string, result *DynamoResult) error {
	if !s.isAlive {
		return errors.New("server has crashed")
	}

	s.mu.Lock()
	result.EntryList = append(result.EntryList, s.store[key]...)
	s.mu.Unlock()
	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		isAlive:        true,
		store:          make(map[string][]ObjectEntry),
		futureGossip:   make([]map[string][]ObjectEntry, len(preferenceList)),
	}
}

func ServeDynamoServer(dynamoServer *DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
