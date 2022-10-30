package emitter

import (
	"fmt"
	"reflect"
	"runtime"
)

func isStructPointer(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
}

func GetStructTypeString(_type reflect.Type) string {
	if isStructPointer(_type) {
		_type = _type.Elem()
	}
	return fmt.Sprintf("%s/%s", _type.PkgPath(), _type.Name())
}

func getFuncName(fn interface{}) string {
	t := reflect.ValueOf(fn)
	if t.Kind() == reflect.Func {
		return runtime.FuncForPC(t.Pointer()).Name()
	}
	return ""
}
