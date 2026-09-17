package network

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	graphsync "github.com/filecoin-project/boost-graphsync"
	gsmsg "github.com/filecoin-project/boost-graphsync/message"
	"github.com/filecoin-project/boost-graphsync/testutil"
)

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan struct{}
	lastMessage     gsmsg.GraphSyncMessage
	lastSender      peer.ID
	connectedPeers  chan peer.ID
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {
	r.lastSender = sender
	r.lastMessage = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveError(_ peer.ID, _ error) {
}

func (r *receiver) Connected(p peer.ID) {
	r.connectedPeers <- p
}

func (r *receiver) Disconnected(p peer.ID) {
}

// countingReceiver only records that a message arrived, so that it can be used
// from the several concurrent stream handlers that a multi-message test
// produces without racing on shared state.
type countingReceiver struct {
	messageReceived chan struct{}
}

func (r *countingReceiver) ReceiveMessage(
	ctx context.Context,
	_ peer.ID,
	_ gsmsg.GraphSyncMessage) {
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *countingReceiver) ReceiveError(peer.ID, error) {}
func (r *countingReceiver) Connected(peer.ID)           {}
func (r *countingReceiver) Disconnected(peer.ID)        {}

func TestMessageSendAndReceive(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New()

	host1, err := mn.GenPeer()
	require.NoError(t, err)
	host2, err := mn.GenPeer()
	require.NoError(t, err)
	err = mn.LinkAll()
	require.NoError(t, err)
	gsnet1 := NewFromLibp2pHost(host1)
	gsnet2 := NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan struct{}),
		connectedPeers:  make(chan peer.ID, 2),
	}
	gsnet1.SetDelegate(r)
	gsnet2.SetDelegate(r)

	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	status := graphsync.RequestAcknowledged

	builder := gsmsg.NewBuilder()
	builder.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	builder.AddResponseCode(id, status)
	builder.AddExtensionData(id, extension)
	sent, err := builder.Build()
	require.NoError(t, err)

	err = gsnet1.ConnectTo(ctx, host2.ID())
	require.NoError(t, err, "did not connect peers")

	err = gsnet1.SendMessage(ctx, host2.ID(), sent)
	require.NoError(t, err)

	testutil.AssertDoesReceive(ctx, t, r.messageReceived, "message did not send")

	require.Equal(t, host1.ID(), r.lastSender, "incorrect host sent message")

	received := r.lastMessage

	sentRequests := sent.Requests()
	require.Len(t, sentRequests, 1, "did not add request to sent message")
	sentRequest := sentRequests[0]
	receivedRequests := received.Requests()
	require.Len(t, receivedRequests, 1, "did not add request to received message")
	receivedRequest := receivedRequests[0]
	require.Equal(t, sentRequest.ID(), receivedRequest.ID())
	require.Equal(t, sentRequest.Type(), receivedRequest.Type())
	require.Equal(t, sentRequest.Root().String(), receivedRequest.Root().String())
	require.Equal(t, sentRequest.Selector(), receivedRequest.Selector())

	sentResponses := sent.Responses()
	require.Len(t, sentResponses, 1, "did not add response to sent message")
	sentResponse := sentResponses[0]
	receivedResponses := received.Responses()
	require.Len(t, receivedResponses, 1, "did not add response to received message")
	receivedResponse := receivedResponses[0]
	extensionData, found := receivedResponse.Extension(extensionName)
	require.Equal(t, sentResponse.RequestID(), receivedResponse.RequestID())
	require.Equal(t, sentResponse.Status(), receivedResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	for i := 0; i < 2; i++ {
		testutil.AssertDoesReceive(ctx, t, r.connectedPeers, "peers were not notified")
	}

}

// TestMaxRequestsPerPeerSecondOption verifies that the per-peer request rate
// limit reaches the message read loop, and that a message is charged for the
// number of requests it carries rather than for being a message at all.
func TestMaxRequestsPerPeerSecondOption(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New()

	host1, err := mn.GenPeer()
	require.NoError(t, err)
	host2, err := mn.GenPeer()
	require.NoError(t, err)
	err = mn.LinkAll()
	require.NoError(t, err)

	gsnet1 := NewFromLibp2pHost(host1)
	// the receiving network admits ten requests per second from any one peer
	gsnet2 := NewFromLibp2pHost(host2, MaxRequestsPerPeerSecond(10))

	r := &countingReceiver{messageReceived: make(chan struct{}, 8)}
	gsnet1.SetDelegate(r)
	gsnet2.SetDelegate(r)

	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()

	messageWithRequests := func(n int) gsmsg.GraphSyncMessage {
		b := gsmsg.NewBuilder()
		for i := 0; i < n; i++ {
			b.AddRequest(gsmsg.NewRequest(graphsync.NewRequestID(), root, selector, 0))
		}
		msg, err := b.Build()
		require.NoError(t, err)
		return msg
	}

	err = gsnet1.ConnectTo(ctx, host2.ID())
	require.NoError(t, err, "did not connect peers")

	// Sixteen requests are sent across three messages against an allowance of
	// ten. Each SendMessage opens its own stream, so the arrival order is not
	// fixed, but in every ordering exactly two of the three messages fit and
	// the last is refused whole. Had each message cost one token regardless of
	// its contents, all three would have been delivered.
	for _, n := range []int{6, 6, 4} {
		require.NoError(t, gsnet1.SendMessage(ctx, host2.ID(), messageWithRequests(n)))
	}

	testutil.AssertDoesReceive(ctx, t, r.messageReceived, "first message within the rate limit was not delivered")
	testutil.AssertDoesReceive(ctx, t, r.messageReceived, "second message within the rate limit was not delivered")

	// Give the over-rate message time to be read and dropped, so that the
	// emptiness check cannot pass merely because it is still in flight. The
	// refill rate is ten per second, so 200ms returns two tokens, far short of
	// the four the smallest of the three messages needs.
	time.Sleep(200 * time.Millisecond)
	testutil.AssertChannelEmpty(t, r.messageReceived, "a message beyond the rate limit was delivered")
}

// TestRequestRateLimitDisabledByDefault documents that the per-peer request rate
// limit is opt-in: a network built without the option admits messages at any
// rate.
func TestRequestRateLimitDisabledByDefault(t *testing.T) {
	require.Zero(t, DefaultMaxRequestsPerPeerSecond,
		"the rate limit is opt-in, so its default must be zero")

	mn := mocknet.New()
	host, err := mn.GenPeer()
	require.NoError(t, err)

	gsnet := NewFromLibp2pHost(host).(*libp2pGraphSyncNetwork)
	// the read loop gates on this field, so it is the value that decides whether a
	// message is ever charged at all
	require.Zero(t, gsnet.maxRequestsPerPeerSecond,
		"a network built without the option must not rate limit")
	for i := 0; i < 1000; i++ {
		require.True(t, gsnet.requestLimiter.allow(host.ID(), 1000),
			"the limiter of a network built without the option must admit everything")
	}
}

// TestMaxRequestsPerPeerSecondGatesTheReadLoop documents that the read loop
// only consults the limiter when the configured rate is positive, so a disabled
// network never touches the limiter on the message path.
func TestMaxRequestsPerPeerSecondGatesTheReadLoop(t *testing.T) {
	for _, limit := range []int{0, -1} {
		mn := mocknet.New()
		host, err := mn.GenPeer()
		require.NoError(t, err)

		gsnet := NewFromLibp2pHost(host, MaxRequestsPerPeerSecond(limit)).(*libp2pGraphSyncNetwork)
		require.Equal(t, limit, gsnet.maxRequestsPerPeerSecond)
		require.Empty(t, gsnet.requestLimiter.peers,
			"a disabled limiter should not track peers even after construction")
	}
}
