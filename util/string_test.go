package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplit(t *testing.T) {
	assert.Equal(t, Split("", ' '), []string{})
	assert.Equal(t, Split("$1='',$2='0',$3=','", ','), []string{"$1=''", "$2='0'", "$3=','"})
	assert.Equal(t, Split("$1=NULL    $2=' ' $3=\"      \"", ' '), []string{"$1=NULL", "$2=' '", "$3=\"      \""})
}
