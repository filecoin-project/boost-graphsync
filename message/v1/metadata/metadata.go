package metadata

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/message"
)

// Item is a single link traversed in a repsonse
type Item struct {
	Link         cid.Cid
	BlockPresent bool
}

// Metadata is information about metadata contained in a response, which can be
// serialized back and forth to bytes
type Metadata []Item

func (md Metadata) ToGraphSyncMetadata() []message.GraphSyncLinkMetadatum {
	if len(md) == 0 {
		return nil
	}
	gsm := make([]message.GraphSyncLinkMetadatum, 0, len(md))
	for _, ii := range md {
		action := graphsync.LinkActionPresent
		if !ii.BlockPresent {
			action = graphsync.LinkActionMissing
		}
		gsm = append(gsm, message.GraphSyncLinkMetadatum{Link: ii.Link, Action: action})
	}
	return gsm
}

// DecodeMetadata assembles metadata from a raw byte array, first deserializing
// as a node and then assembling into a metadata struct.
func DecodeMetadata(data datamodel.Node) (Metadata, error) {
	if data == nil {
		return nil, nil
	}
	builder := Prototype.Metadata.Representation().NewBuilder()
	err := builder.AssignNode(data)
	if err != nil {
		return nil, err
	}
	metadata := bindnode.Unwrap(builder.Build())
	return *(metadata.(*Metadata)), nil
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata) datamodel.Node {
	return bindnode.Wrap(&entries, Prototype.Metadata.Type())
}