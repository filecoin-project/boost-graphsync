package dedupkey

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeDedupKey(t *testing.T) {
	encoded, err := EncodeDedupKey("key")
	require.NoError(t, err)
	decoded, err := DecodeDedupKey(encoded)
	require.NoError(t, err)
	require.Equal(t, "key", decoded)
}

func TestDecodeDedupKeyNullNode(t *testing.T) {
	// A present-but-null extension value must decode to an error, not panic.
	decoded, err := DecodeDedupKey(nil)
	require.ErrorIs(t, err, ErrNilNode)
	require.Empty(t, decoded)
}
