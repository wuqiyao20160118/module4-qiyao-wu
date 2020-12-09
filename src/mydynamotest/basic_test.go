package mydynamotest

import (
	"log"
	"mydynamo"
	"sync"
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
}

func TestBasicPut1(t *testing.T) {
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

// W: 3, R: 1
func TestAdvancedPut(t *testing.T) {
	t.Logf("Starting Advanced Put test")

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
	clientInstance1 := MakeConnectedClient(8080)
	ctxMap1 := make(map[string]mydynamo.Context)
	clientInstance2 := MakeConnectedClient(8081)
	ctxMap2 := make(map[string]mydynamo.Context)
	clientInstance3 := MakeConnectedClient(8082)
	ctxMap3 := make(map[string]mydynamo.Context)
	clientInstance4 := MakeConnectedClient(8083)
	ctxMap4 := make(map[string]mydynamo.Context)
	clientInstance5 := MakeConnectedClient(8084)
	ctxMap5 := make(map[string]mydynamo.Context)

	wg := new(sync.WaitGroup)
	wg.Add(4)

	go func() {
		//Put a value on key "s1"
		clientInstance1.Put(PutFreshContext("s1", []byte("abc")))
		log.Println("********************clientInstance1 Put done.*****************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance1.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance2.Put(PutFreshContext("s1", []byte("bc")))
		log.Println("********************clientInstance2 Put done.*****************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance2.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

		clientInstance2.Put(PutFreshContext("s2", []byte("123")))
		log.Println("******************clientInstance2 Put done.********************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr = clientInstance2.Get("s2")
		gotValue = *gotValuePtr
		// Get the returned vector clock list
		ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance3.Put(PutFreshContext("s1", []byte("ac")))
		log.Println("****************clientInstance3 Put done.**************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance3.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap3["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance5.Put(PutFreshContext("s1", []byte("bcdefg")))
		log.Println("**************clientInstance5 Put done.************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance5.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap5["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

		clientInstance5.Put(PutFreshContext("s3", []byte("xxxxxxx")))
		log.Println("**********clientInstance5 Put done.**************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr = clientInstance5.Get("s3")
		gotValue = *gotValuePtr
		// Get the returned vector clock list
		ctxMap5["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	//wait for all servers to finish
	wg.Wait()

	log.Println("======================= clientInstance1 Test One Begins ==========================")
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 2 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 2, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 1)
	}
	ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap1["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance1 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance2 Test One Begins ==========================")
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 3 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 3, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 2)
	}
	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance2 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance3 Test One Begins ==========================")
	gotValuePtr = clientInstance3.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 3 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 3, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 2)
	}
	ctxMap3["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance3.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap3["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance3 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance4 Test One Begins ==========================")
	gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 2 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 2, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 1)
	}
	ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance4.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	ctxMap4["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance4 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance5 Test One Begins ==========================")
	gotValuePtr = clientInstance5.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 2 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 2, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 1)
	}
	ctxMap5["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance5.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	ctxMap5["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance5 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	clientInstance2.Gossip()
	log.Println("======================= clientInstance1 Test Two Begins ==========================")
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 3 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 3, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 2)
	}
	ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap1["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap1["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance1 Test Two Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance2 Test Two Begins ==========================")
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 3 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 3, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 2)
	}
	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance2 Test Two Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance3 Test Two Begins ==========================")
	gotValuePtr = clientInstance3.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 3)
	}
	ctxMap3["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance3.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap3["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance3.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap3["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance3 Test Two Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance4 Test Two Begins ==========================")
	gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 3)
	}
	ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance4.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	ctxMap4["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance4.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	ctxMap4["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance4 Test Two Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance5 Test Two Begins ==========================")
	gotValuePtr = clientInstance5.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 3)
	}
	ctxMap5["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance5.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	ctxMap5["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance5.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	ctxMap5["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance5 Test Two Done <<<<<<<<<<<<<<<<<<<<<<<<")

	wg = new(sync.WaitGroup)
	wg.Add(5)
	go func() {
		clientInstance1.Gossip()
		wg.Done()
	}()
	go func() {
		clientInstance2.Gossip()
		wg.Done()
	}()
	go func() {
		clientInstance3.Gossip()
		wg.Done()
	}()
	go func() {
		clientInstance4.Gossip()
		wg.Done()
	}()
	go func() {
		clientInstance5.Gossip()
		wg.Done()
	}()
	wg.Wait()

	log.Println("======================= clientInstance1 Test Three Begins ==========================")
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 3)
	}
	ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap1["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap1["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance1 Test Three Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance2 Test Three Begins ==========================")
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 3)
	}
	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap2["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance2 Test Three Done <<<<<<<<<<<<<<<<<<<<<<<<")
}

// W: 3, R: 3
func TestAdvancedPut2(t *testing.T) {
	t.Logf("Starting Advanced Put test")

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
	clientInstance1 := MakeConnectedClient(8080)
	ctxMap1 := make(map[string]mydynamo.Context)
	clientInstance2 := MakeConnectedClient(8081)
	ctxMap2 := make(map[string]mydynamo.Context)
	clientInstance3 := MakeConnectedClient(8082)
	ctxMap3 := make(map[string]mydynamo.Context)
	clientInstance4 := MakeConnectedClient(8083)
	ctxMap4 := make(map[string]mydynamo.Context)
	clientInstance5 := MakeConnectedClient(8084)
	ctxMap5 := make(map[string]mydynamo.Context)

	wg := new(sync.WaitGroup)
	wg.Add(4)

	go func() {
		//Put a value on key "s1"
		clientInstance1.Put(PutFreshContext("s1", []byte("abc")))
		log.Println("********************clientInstance1 Put done.*****************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance1.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance2.Put(PutFreshContext("s1", []byte("bc")))
		log.Println("********************clientInstance2 Put done.*****************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance2.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

		clientInstance2.Put(PutFreshContext("s2", []byte("123")))
		log.Println("******************clientInstance2 Put done.********************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr = clientInstance2.Get("s2")
		gotValue = *gotValuePtr
		// Get the returned vector clock list
		ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance3.Put(PutFreshContext("s1", []byte("ac")))
		log.Println("****************clientInstance3 Put done.**************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance3.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap3["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	go func() {
		//Put a value on key "s1"
		clientInstance5.Put(PutFreshContext("s1", []byte("bcdefg")))
		log.Println("**************clientInstance5 Put done.************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr := clientInstance5.Get("s1")
		gotValue := *gotValuePtr
		// Get the returned vector clock list
		ctxMap5["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

		clientInstance5.Put(PutFreshContext("s3", []byte("xxxxxxx")))
		log.Println("**********clientInstance5 Put done.**************")
		//Get the value back, and check if we successfully retrieved the correct value
		gotValuePtr = clientInstance5.Get("s3")
		gotValue = *gotValuePtr
		// Get the returned vector clock list
		ctxMap5["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
		log.Println("===============================================")
		wg.Done()
	}()

	//wait for all servers to finish
	wg.Wait()

	log.Println("======================= clientInstance1 Test One Begins ==========================")
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 3)
	}
	ctxMap1["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	ctxMap1["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance1.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance1 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance1: Failed to get value, index: %d", 0)
	}
	ctxMap1["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance1 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance2 Test One Begins ==========================")
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 3)
	}
	ctxMap2["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance2 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance2: Failed to get value, index: %d", 0)
	}
	ctxMap2["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance2.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap2["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance2 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance3 Test One Begins ==========================")
	gotValuePtr = clientInstance3.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 3)
	}
	ctxMap3["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance3.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance3 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance3: Failed to get value, index: %d", 0)
	}
	ctxMap3["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance3 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance4 Test One Begins ==========================")
	gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 3)
	}
	ctxMap4["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance4.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance4 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("123")) {
		t.Fail()
		t.Logf("clientInstance4: Failed to get value, index: %d", 0)
	}
	ctxMap4["s2"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance4 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")

	log.Println("======================= clientInstance5 Test One Begins ==========================")
	gotValuePtr = clientInstance5.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 4, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("abc")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	if !valuesEqual(gotValue.EntryList[1].Value, []byte("ac")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 1)
	}
	if !valuesEqual(gotValue.EntryList[2].Value, []byte("bc")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 2)
	}
	if !valuesEqual(gotValue.EntryList[3].Value, []byte("bcdefg")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 3)
	}
	ctxMap5["s1"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}

	gotValuePtr = clientInstance5.Get("s3")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("clientInstance5 Get Test Crash: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value. Expected length: %d, but Get: %d", 1, len(gotValue.EntryList))
		for _, content := range gotValue.EntryList {
			t.Logf("%s", string(content.Value))
		}
	}
	if !valuesEqual(gotValue.EntryList[0].Value, []byte("xxxxxxx")) {
		t.Fail()
		t.Logf("clientInstance5: Failed to get value, index: %d", 0)
	}
	ctxMap5["s3"] = mydynamo.Context{Clock: GetAndCombineClocks(gotValue)}
	log.Println(">>>>>>>>>>>>>>>>>>>>>> clientInstance5 Test One Done <<<<<<<<<<<<<<<<<<<<<<<<")
}

func TestPutW2(t *testing.T) {
	t.Logf("Starting PutW2 test")
	cmd := InitDynamoServer("./twoserver.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestPutW2: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestPutW2: Failed to get value")
	}

}

func TestGossip(t *testing.T) {
	t.Logf("Starting Gossip test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossip: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestGossip: Failed to get value")
	}

}

func TestMultipleKeys(t *testing.T) {
	t.Logf("Starting MultipleKeys test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get value")
	}

	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue = *gotValuePtr

	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("efghi")))
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get value")
	}
}

func TestDynamoPaper(t *testing.T) {
	t.Logf("DynamoPaper test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance2 := MakeConnectedClient(8082)

	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get first value")
	}

	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestDynamoPaper: First value doesn't match")
	}
	clientInstance0.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("bcdef")))
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get second value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("bcdef")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Second value doesn't match")
	}

	clientInstance0.Gossip()
	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("cdefg")))
	clientInstance2.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("defgh")))
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get third value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("cdefg")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Third value doesn't match")
	}
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get fourth value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("defgh")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Fourth value doesn't match")
	}
	clientInstance1.Gossip()
	clientInstance2.Gossip()
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get fifth value")
	}
	gotValue = *gotValuePtr
	clockList := make([]mydynamo.VectorClock, 0)
	for _, a := range gotValue.EntryList {
		clockList = append(clockList, a.Context.Clock)
	}
	clockList[0].Combine(clockList)
	combinedClock := clockList[0]
	combinedContext := mydynamo.Context{
		Clock: combinedClock,
	}
	clientInstance0.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zyxw")))
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get sixth value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Sixth value doesn't match")
	}

}

func TestInvalidPut(t *testing.T) {
	t.Logf("Starting repeated Put test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready
	clientInstance := MakeConnectedClient(8080)

	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance.Put(PutFreshContext("s1", []byte("efghi")))
	gotValue := clientInstance.Get("s1")
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestInvalidPut: Got wrong value")
	}
}

func TestGossipW2(t *testing.T) {
	t.Logf("Starting GossipW2 test")
	cmd := InitDynamoServer("./twoserver.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get first element")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get value")
	}
	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("efghi")))

	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get")
	}
	gotValue = *gotValuePtr

	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("GossipW2: Failed to get value")
	}
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get")
	}
	gotValue = *gotValuePtr

	if (len(gotValue.EntryList) != 1) || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("GossipW2: Failed to get value")
	}

}

func TestReplaceMultipleVersions(t *testing.T) {
	t.Logf("Starting ReplaceMultipleVersions test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance1.Put(PutFreshContext("s1", []byte("efghi")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplaceMultipleVersions: Failed to get")
	}

	gotValue := *gotValuePtr
	clockList := make([]mydynamo.VectorClock, 0)
	for _, a := range gotValue.EntryList {
		clockList = append(clockList, a.Context.Clock)
	}
	clockList[0].Combine(clockList)
	combinedClock := clockList[0]
	combinedContext := mydynamo.Context{
		Clock: combinedClock,
	}
	clientInstance1.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zxyw")))
	gotValuePtr = nil
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplaceMultipleVersions: Failed to get")
	}

	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zxyw")) {
		t.Fail()
		t.Logf("testReplaceMultipleVersions: Values don't match")
	}

}

func TestConsistent(t *testing.T) {
	t.Logf("Starting Consistent test")
	cmd := InitDynamoServer("./consistent.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance2 := MakeConnectedClient(8082)
	clientInstance3 := MakeConnectedClient(8083)
	clientInstance4 := MakeConnectedClient(8084)

	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConsistent: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestConsistent: Failed to get value")
	}

	clientInstance3.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("zyxw")))
	clientInstance0.Crash(3)
	clientInstance1.Crash(3)
	clientInstance4.Crash(3)
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConsistent: Failed to get")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw")) {
		t.Fail()
		t.Logf("TestConsistent: Failed to get value")
	}
}
