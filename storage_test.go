package tprstorage

import (
	"testing"
)

func TestInterface(t *testing.T) {
	// This won't compile if Storage doens't fullfil the interface.
	//var _ microstorage.Storage = &Storage{}
}
