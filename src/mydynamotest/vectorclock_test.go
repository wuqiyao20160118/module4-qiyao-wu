package mydynamotest

import (
	"mydynamo"
	"testing"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}

	clock1.Increment("123")
	clock2.Increment("456")
	clock2.Increment("123")
	//Test for basic descendent
	if !clock1.LessThan(clock2) {
		t.Fail()
		t.Logf("Vector Clock 1 should be the descendent of Clock 2")
	}
	clock2.Increment("7890")
	if !clock1.LessThan(clock2) {
		t.Fail()
		t.Logf("Vector Clock 1 should be the descendent of Clock 2")
	}

	clock1.Increment("12")
	//Test for basic concurrency
	if !clock1.Concurrent(clock2) || !clock2.Concurrent(clock1) {
		t.Fail()
		t.Logf("Vector Clocks were not concurrent")
	}

	clock2.Increment("123")
	clock1.Increment("12")
	clock2.Increment("123")
	clock2.Increment("7890")
	clockList := make([]mydynamo.VectorClock, 0)
	clockList = append(clockList, clock2)
	clock1.Combine(clockList)
	if !clock2.LessThan(clock1) {
		t.Fail()
		t.Logf("Vector Clock 2 should be the descendent of Clock 1")
	}
	if clock1.Concurrent(clock2) || clock2.Concurrent(clock1) {
		t.Fail()
		t.Logf("Vector Clocks should not be concurrent")
	}

	clockList = make([]mydynamo.VectorClock, 0)
	clockList = append(clockList, clock1)
	clock2.Combine(clockList)
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}
	if clock1.Concurrent(clock2) || clock2.Concurrent(clock1) {
		t.Fail()
		t.Logf("Vector Clocks should not be concurrent")
	}

	clock3 := mydynamo.NewVectorClock()
	clock3.Increment("321")
	clock3.Increment("321")
	clock3.Increment("456")
	clock3.Increment("456")
	if !clock3.Concurrent(clock1) || !clock3.Concurrent(clock2) {
		t.Fail()
		t.Logf("Vector Clocks should be concurrent")
	}

	clock1.Increment("987")
	clockList = make([]mydynamo.VectorClock, 0)
	clockList = append(clockList, clock1)
	clockList = append(clockList, clock2)
	clock3.Combine(clockList)
	if !clock2.LessThan(clock3) || !clock1.LessThan(clock3) {
		t.Fail()
		t.Logf("Vector Clock 1 and 2 should be the descendent of Clock 3")
	}
}
