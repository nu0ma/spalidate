package main

import (
	"fmt"
	"reflect"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	val := &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: "test",
		},
	}
	
	fmt.Printf("Type: %T\n", val)
	fmt.Printf("Type string: %s\n", reflect.TypeOf(val).String())
	fmt.Printf("String value: %s\n", val.GetStringValue())
}