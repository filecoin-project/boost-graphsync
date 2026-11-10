package donotsendfirstblocks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeDoNotSendFirstBlocks(t *testing.T) {
	decoded, err := DecodeDoNotSendFirstBlocks(EncodeDoNotSendFirstBlocks(7))
	require.NoError(t, err)
	require.Equal(t, int64(7), decoded)
}

func TestDecodeDoNotSendFirstBlocksNullNode(t *testing.T) {
	// A present-but-null extension value must decode to an error, not panic.
	decoded, err := DecodeDoNotSendFirstBlocks(nil)
	require.ErrorIs(t, err, ErrNilNode)
	require.Equal(t, int64(0), decoded)
}
