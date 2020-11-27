package mydynamo

type VectorClock struct {
	VectorMap  map[string]int
	Size       int
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	// At first, the vector clock is empty.
	return VectorClock{
		VectorMap: make(map[string]int),
		Size: 0,
	}
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	if s.Size > otherClock.Size {
		return false
	}

	isEqual := true
	for nodeId, version := range s.VectorMap {
		otherVersion, ok := otherClock.VectorMap[nodeId]
		if !ok {
			return false
		}
		if version > otherVersion {
			return false
		} else if version < otherVersion {
			isEqual = false
		}
	}
	if s.Size < otherClock.Size {
		return true
	}
	return !isEqual
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	return !s.LessThan(otherClock) && !otherClock.LessThan(s) && !s.Equals(otherClock)
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	_, ok := s.VectorMap[nodeId]
	if !ok {
		s.VectorMap[nodeId] = 1
		s.Size += 1
	} else {
		s.VectorMap[nodeId] += 1
	}
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	for i := 0; i < len(clocks); i++ {
		for nodeId, version := range clocks[i].VectorMap {
			nowVersion, ok := s.VectorMap[nodeId]
			if !ok || nowVersion < version {
				s.VectorMap[nodeId] = version
				if !ok {
					s.Size += 1
				}
			}
		}
	}
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	if s.Size != otherClock.Size {
		return false
	}
	for id, version := range s.VectorMap {
		otherVersion, ok := otherClock.VectorMap[id]
		if !ok {
			return false
		}

		if version != otherVersion {
			return false
		}
	}
	return true
}
