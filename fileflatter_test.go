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
	fmt.Println()

	fm, err := FlatContent(f)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range fm {
		fmt.Println(k, v)
	}

	fmt.Println()

	fmt.Printf("%+v", GetCSVObjects(fm, 1, 2, 0)[2])

	fmt.Println()

	fmt.Printf("%+v", GetCSVObjects(fm)[2])
}
