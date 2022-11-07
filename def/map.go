package def

import (
	"mapreduce-go/mapreduce"
	"strings"
	"unicode"
)

func Map(filename, contents string) []mapreduce.KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool { return !unicode.IsLetter(r) })

	keyValues := []mapreduce.KeyValue{}

	for _, word := range words {
		keyvalue := mapreduce.KeyValue{Key: word, Value: "1"}
		keyValues = append(keyValues, keyvalue)
	}
	return keyValues
}
