package shred

type Record map[string]interface{}

func (r Record) Get(key string) interface{} {
	val, _ := r[key]
	return val
}

func (r Record) GetOr(key string, or interface{}) interface{} {
	if val, exists := r[key]; exists {
		return val
	}

	return or
}

func (r Record) Int(key string) int {
	i, _ := r.Get(key).(int)
	return i
}

func (r Record) IntOr(key string, or int) int {
	if i, ok := r.Get(key).(int); ok {
		return i
	}

	return or
}

func (r Record) String(key string) string {
	s, _ := r.Get(key).(string)
	return s
}

func (r Record) StringOr(key string, or string) string {
	if s, ok := r.Get(key).(string); ok {
		return s
	}

	return or
}

func (r Record) Clone() Record {
	clone := Record{}
	for k, v := range r {
		clone[k] = v
	}
	return clone
}

func (r Record) Set(key string, value interface{}) Record {
	clone := r.Clone()
	clone[key] = value
	return clone
}

func (r *Record) Merge(rec Record) Record {
	clone := r.Clone()
	for k, v := range rec {
		clone[k] = v
	}
	return clone
}
