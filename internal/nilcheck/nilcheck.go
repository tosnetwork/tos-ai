// Package nilcheck provides the construction-time interface validation used
// at operator-controlled dependency injection boundaries.
package nilcheck

import "reflect"

// IsNil reports whether value is nil or contains a typed nil value. A plain
// interface comparison is insufficient for dependencies such as (*T)(nil).
func IsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
