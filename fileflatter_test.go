package fileflatter

import (
	"fmt"
	"os"
	"testing"
)

func TestFlatContent(t *testing.T) {
	f, err := os.Open("./data/sample.csv")
	if err == nil {
		fmt.Println("sample type:", TxtType(f))
	}
	fm, err := FlatContent(f)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(fm)

	fmt.Println()

	fmt.Printf("%+v", GetCSVObjects(fm, 0, 1)[0])
}
