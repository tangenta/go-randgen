package sqlgen

type ScopeKeyType int8

const (
	ScopeKeyCurrentTable ScopeKeyType = iota
	ScopeKeyCurrentColumn
	ScopeKeyCurrentIndex

	ScopeKeySelectedCols

	ScopeKeyTableUniqID
	ScopeKeyColumnUniqID
	ScopeKeyIndexUniqID
)

type ScopeObj struct {
	obj interface{}
}

func NewScopeObj(obj interface{}) ScopeObj {
	return ScopeObj{obj}
}

func (s ScopeObj) IsNil() bool {
	return s.obj == nil
}

func (s ScopeObj) ToTable() *Table {
	return s.obj.(*Table)
}

func (s ScopeObj) ToColumn() *Column {
	return s.obj.(*Column)
}

func (s ScopeObj) ToIndex() *Index {
	return s.obj.(*Index)
}

func (s ScopeObj) ToInt() int {
	return s.obj.(int)
}

func (s ScopeObj) ToColumns() []*Column {
	return s.obj.([]*Column)
}

func (s *State) CreateScope() {
	s.scope = append(s.scope, make(map[ScopeKeyType]ScopeObj))
}

func (s *State) DestroyScope() {
	if len(s.scope) == 0 {
		return
	}
	s.scope = s.scope[:len(s.scope)-1]
}

func (s *State) Store(key ScopeKeyType, val ScopeObj) {
	current := s.scope[len(s.scope)-1]
	current[key] = val
}

func (s *State) Search(key ScopeKeyType) ScopeObj {
	for i := len(s.scope) - 1; i >= 0; i-- {
		current := s.scope[i]
		if v, ok := current[key]; ok {
			return v
		}
	}
	return ScopeObj{}
}

func (s *State) CreateScopeAndStore(key ScopeKeyType, val ScopeObj) {
	s.CreateScope()
	s.Store(key, val)
}

func (s *State) AllocGlobalID(key ScopeKeyType) int {
	var result int
	if v := s.Search(key); !v.IsNil() {
		result = v.ToInt()
	} else {
		result = 0
	}
	s.Store(key, NewScopeObj(result+1))
	return result
}
