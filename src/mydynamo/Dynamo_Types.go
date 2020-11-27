package mydynamo

//Placeholder type for RPC functions that don't need an argument list or a return value
type Empty struct{}

//Context associated with some value
type Context struct {
	Clock VectorClock
}

//Information needed to connect to a DynamoNOde
type DynamoNode struct {
	Address string
	Port    string
}

//A single value, as well as the Context associated with it
type ObjectEntry struct {
	Context Context
	Value   []byte
}

//Result of a Get operation, a list of ObjectEntry structs
type DynamoResult struct {
	EntryList []ObjectEntry
}

func (r DynamoResult) Len() int {
	return len(r.EntryList)
}

func (r DynamoResult) Swap(i, j int) {
	r.EntryList[i], r.EntryList[j] = r.EntryList[j], r.EntryList[i]
}

func (r DynamoResult) Less(i, j int) bool {
	r1 := r.EntryList[i].Value
	r2 := r.EntryList[j].Value
	ptr := 0
	for ;ptr < len(r1) && ptr < len(r2); ptr++ {
		if r1[ptr] != r2[ptr] {
			return r1[ptr] < r2[ptr]
		}
	}
	return len(r1) <= len(r2)
}

//Arguments required for a Put operation: the key, the context, and the value
type PutArgs struct {
	Key     string
	Context Context
	Value   []byte
}

type BatchReplicateArgs struct {
	EntryList    []ObjectEntry
	Key          string
}
