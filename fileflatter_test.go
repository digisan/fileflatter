package fileflatter

import (
	"fmt"
	"os"
	"testing"

	dt "github.com/digisan/gotk/data-type"
)

func TestFlatContent(t *testing.T) {

	f, err := os.Open("./data/sample.csv")
	if err == nil {
		fmt.Println("sample type:", dt.DataType(f))
	}
	fmt.Println()

	fm, err := FlatContent(f, false)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range fm {
		fmt.Println(k, v)
	}

	fmt.Println()
	fmt.Println()

	fmt.Printf("%+v\n", GetCSVObjects(fm, 1, 2, 0)[1])

	// fmt.Println()

	// fmt.Printf("%+v", GetCSVObjects(fm)[2])

	// PrintFlat(fm)
}
