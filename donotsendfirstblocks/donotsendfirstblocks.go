package donotsendfirstblocks

import (
	"errors"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// ErrNilNode is returned when decoding a null extension value
var ErrNilNode = errors.New("cannot decode do-not-send-first-blocks from a null node")

// EncodeDoNotSendFirstBlocks returns encoded cbor data for the given number
// of blocks to skip
func EncodeDoNotSendFirstBlocks(skipBlockCount int64) datamodel.Node {
	return basicnode.NewInt(skipBlockCount)
}

// DecodeDoNotSendFirstBlocks returns the number of blocks to skip
func DecodeDoNotSendFirstBlocks(data datamodel.Node) (int64, error) {
	// The graphsync schema declares extension values as nullable Any, so a
	// present-but-null extension arrives here as a nil node. Calling a method
	// on it would panic; report it as a decode error instead.
	if data == nil {
		return 0, ErrNilNode
	}
	return data.AsInt()
}
