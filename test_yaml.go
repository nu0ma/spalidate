package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

func main() {
	yamlContent := `
test:
  null_value: null
  string_value: "test"
  int_value: 42
`
	
	var data map[string]map[string]interface{}
	err := yaml.Unmarshal([]byte(yamlContent), &data)
	if err != nil {
		panic(err)
	}
	
	fmt.Printf("null_value: %v (%T)\n", data["test"]["null_value"], data["test"]["null_value"])
	fmt.Printf("string_value: %v (%T)\n", data["test"]["string_value"], data["test"]["string_value"])
	fmt.Printf("int_value: %v (%T)\n", data["test"]["int_value"], data["test"]["int_value"])
	
	if data["test"]["null_value"] == nil {
		fmt.Println("null_value is nil: true")
	} else {
		fmt.Println("null_value is nil: false")
	}
}