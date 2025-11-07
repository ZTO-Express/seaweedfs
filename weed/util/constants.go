package util

import (
	"fmt"
)

//const HttpStatusCancelled = 499

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.69)
	VERSION        = VERSION_NUMBER
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
