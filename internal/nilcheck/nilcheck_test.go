package nilcheck

import "testing"

type sample struct{}

func TestIsNilDistinguishesTypedNilAndConcreteValues(t *testing.T) {
	var pointer *sample
	var function func()
	var values map[string]string
	for name, value := range map[string]any{
		"nil": nil, "pointer": pointer, "function": function, "map": values,
	} {
		if !IsNil(value) {
			t.Fatalf("%s typed nil was accepted", name)
		}
	}
	for name, value := range map[string]any{
		"pointer": &sample{}, "integer": 1, "string": "value", "function": func() {},
	} {
		if IsNil(value) {
			t.Fatalf("%s concrete value was rejected", name)
		}
	}
}
