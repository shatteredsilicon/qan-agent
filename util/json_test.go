package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsJSONKeyExists(t *testing.T) {
	assert.Equal(t, IsJSONKeyExists(nil, "a", 0), false)
	assert.Equal(t, IsJSONKeyExists(1, "a", 0), false)
	assert.Equal(t, IsJSONKeyExists(map[string]interface{}{"a": 1}, "a", MAX_OBJ_DEPTH), false)
	assert.Equal(t, IsJSONKeyExists(map[string]interface{}{"b": 1}, "a", 0), false)
	assert.Equal(t, IsJSONKeyExists(map[string]interface{}{"b": 1}, "b", 0), true)
	assert.Equal(t, IsJSONKeyExists(map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 1}}}, "c", 0), true)
}
