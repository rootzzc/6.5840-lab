package main

import (
	"fmt"
	"regexp"
	"strings"

	"6.5840/mr"
)

var pattern string

func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	lines := strings.Split(contents, "\n")
	for i, line := range lines {
		matched, err := regexp.MatchString("about", line)
		if matched == false || err != nil {
			continue
		}

		k := fmt.Sprintf("%s: line %d", filename, i)
		kva = append(kva, mr.KeyValue{Key: k, Value: line})
	}
	return kva
}

func Reduce(key string, values []string) string {
	return values[0]
}
