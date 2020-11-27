package mydynamotest

import (
	"log"
	"mydynamo"
	"testing"
	"time"
)

func TestBasicPut(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)
	ctxMap := make(map[string]mydynamo.Context)

	//Put a value on key "s1"
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	// Get the returned vector clock list
	//vecClockList := make([]mydynamo.VectorClock, 0)
	//for i := 0; i < len(gotValue.EntryList); i++ {
	//	vecClockList = append(vecClockList, gotValue.EntryList[i].Context.Clock)
	//}
	//clock := mydynamo.NewVectorClock()
	//clock.Combine(vecClockList)
	ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	clientInstance.Put(PutFreshContext("s2", []byte("a")))
	gotValuePtr = clientInstance.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("a")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	// Get the returned vector clock list
	ctxMap["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	//ctxMap["s2"] = gotValue.EntryList[0].Context
	log.Println("===============================================")

	clientInstance.Put(PutFreshContext("s3", []byte("b")))
	gotValuePtr = clientInstance.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("b")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	ctx, _ := ctxMap["s1"]
	clientInstance.Put(PutWithContext("s1", []byte("abc"), ctx))
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	toCheckClock := ctxMap["s1"]
	log.Println("===============================================")

	ctx, _ = ctxMap["s2"]
	clientInstance.Put(PutWithContext("s2", []byte("abcde"), ctx))
	gotValuePtr = clientInstance.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	clientInstance.Put(PutFreshContext("s4", []byte("c")))
	gotValuePtr = clientInstance.Get("s4")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("c")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s4"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	//Test for equality
	if !toCheckClock.Clock.Equals(ctxMap["s1"].Clock) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}
	log.Println("===============================================")

	log.Println("===================== start crash test ==========================")
	clientInstance.Crash(2)
	time.Sleep(time.Second * time.Duration(1))
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr != nil {
		t.Fail()
		t.Logf("TestBasicCrash: Server node should be down.")
	}
	time.Sleep(time.Millisecond * time.Duration(1500))
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	log.Println("====================== start basic gossip test =========================")
	clientInstance.Gossip()
	clientInstance2 := MakeConnectedClient(8081)
	ctxMap2 := make(map[string]mydynamo.Context)
	clientInstance4 := MakeConnectedClient(8083)
	ctxMap4 := make(map[string]mydynamo.Context)
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	gotValuePtr = clientInstance4.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap4["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
	ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println("===============================================")

	log.Println("===================== Basic Put + Get + Gossip test ==========================")
	go func() {
		ctx, _ = ctxMap["s1"]
		clientInstance.Put(PutWithContext("s1", []byte("abcd"), ctx))
		gotValuePtr = clientInstance.Get("s1")
		if gotValuePtr == nil {
			t.Fail()
			t.Logf("TestBasicPut: Returned nil")
		}
		gotValue = *gotValuePtr
		if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcd")) {
			t.Fail()
			t.Logf("TestBasicPut: Failed to get value. Expected: %v, Get: %v", []byte("abcd"), gotValue.EntryList[0].Value)
		}
		ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	}()
	go func() {
		ctx2, _ := ctxMap2["s1"]
		clientInstance2.Put(PutWithContext("s1", []byte("abce"), ctx2))
		gotValuePtr = clientInstance2.Get("s1")
		if gotValuePtr == nil {
			t.Fail()
			t.Logf("TestBasicPut: Returned nil")
		}
		gotValue = *gotValuePtr
		if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abce")) {
			t.Fail()
			t.Logf("TestBasicPut: Failed to get value. Expected: %v, Get: %v", []byte("abce"), gotValue.EntryList[0].Value)
		}
		ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	}()
	go func() {
		ctx4, _ := ctxMap4["s1"]
		clientInstance4.Put(PutWithContext("s1", []byte("abcde"), ctx4))
		gotValuePtr = clientInstance4.Get("s1")
		if gotValuePtr == nil {
			t.Fail()
			t.Logf("TestBasicPut: Returned nil")
		}
		gotValue = *gotValuePtr
		if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
			t.Fail()
			t.Logf("TestBasicPut: Failed to get value. Expected: %s, Get: %s", string([]byte("abcde")), string(gotValue.EntryList[0].Value))
		}
		ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

		ctx4, _ = ctxMap4["s1"]
		clientInstance4.Put(PutWithContext("s1", []byte("abe"), ctx4))
		gotValuePtr = clientInstance4.Get("s1")
		if gotValuePtr == nil {
			t.Fail()
			t.Logf("TestBasicPut: Failed to get value. Expected: %v, Get: %v", []byte("abe"), gotValue.EntryList[0].Value)
		}
		gotValue = *gotValuePtr
		if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abe")) {
			t.Fail()
			t.Logf("TestBasicPut: Failed to get value")
		}
		ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	}()
	time.Sleep(time.Millisecond * time.Duration(500))
	log.Println("===================== Begin Gossip operation ==========================")
	clientInstance.Gossip()
	clientInstance2.Gossip()
	clientInstance4.Gossip()
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 3 {
		t.Fail()
		t.Logf("TestCombine: Failed to get value. Expected length: %d, but Get: %d", 3, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abcd")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("abce")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("abe")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value, index: %d", 2)
	}

	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	toCheckClock = ctxMap2["s1"]
	log.Println("===============================================")

	log.Println("========================Checking Vector Clocks========================")
	gotValuePtr = clientInstance.Get("s1")
	ctxMap["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	//Test for equality
	if !toCheckClock.Clock.Equals(ctxMap["s1"].Clock) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}
	log.Println("===============================================")
}
