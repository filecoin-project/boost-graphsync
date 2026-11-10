package dedupkey

import (
	"errors"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// ErrNilNode is returned when decoding a null extension value
var ErrNilNode = errors.New("cannot decode dedup key from a null node")

// EncodeDedupKey returns encoded cbor data for string key
func EncodeDedupKey(key string) (datamodel.Node, error) {
	nb := basicnode.Prototype.String.NewBuilder()
	err := nb.AssignString(key)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// DecodeDedupKey returns a string key decoded from cbor data
func DecodeDedupKey(data datamodel.Node) (string, error) {
	// The graphsync schema declares extension values as nullable Any, so a
	// present-but-null extension arrives here as a nil node. Calling a method
	// on it would panic; report it as a decode error instead.
	if data == nil {
		return "", ErrNilNode
	}
	return data.AsString()
}
