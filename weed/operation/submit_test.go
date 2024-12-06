package operation

import (
	"fmt"
	"testing"
)

func TestSubmitFiles(t *testing.T) {

}

func TestSs(t *testing.T) {
	con := calConcurrent(4*1024*1024, 100)
	fmt.Println("con", con)
}
