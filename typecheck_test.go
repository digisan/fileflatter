package fileflatter

import (
	"fmt"
	"testing"
)

func TestTypeCheck(t *testing.T) {
	fmt.Println(IsJSON(""))
	fmt.Println(IsXML(""))
	fmt.Println(IsCSV(""))
	fmt.Println(IsTOML(""))
}
