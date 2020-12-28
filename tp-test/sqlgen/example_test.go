package sqlgen

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	gen := NewGenerator(NewState())
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}
