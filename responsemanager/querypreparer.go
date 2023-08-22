package responsemanager

import (
	"context"
	"io"
	"math"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p/core/peer"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/cidset"
	"github.com/filecoin-project/boost-graphsync/dedupkey"
	"github.com/filecoin-project/boost-graphsync/donotsendfirstblocks"
	"github.com/filecoin-project/boost-graphsync/ipldutil"
	gsmsg "github.com/filecoin-project/boost-graphsync/message"
	"github.com/filecoin-project/boost-graphsync/panics"
	"github.com/filecoin-project/boost-graphsync/responsemanager/queryexecutor"
	"github.com/filecoin-project/boost-graphsync/responsemanager/responseassembler"
)

type errorString string

func (e errorString) Error() string {
	return string(e)
}

const errInvalidRequest = errorString("request not valid")

type queryPreparer struct {
	requestHooks RequestHooks
	linkSystem   ipld.LinkSystem
	// maximum number of links to traverse per request. A value of zero = infinity, or no limit=
	maxLinksPerRequest uint64
	panicCallback      panics.CallBackFn
}

func (qe *queryPreparer) prepareQuery(
	ctx context.Context,
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	responseStream responseassembler.ResponseStream,
	signals queryexecutor.ResponseSignals) (ipld.BlockReadOpener, ipldutil.Traverser, bool, error) {
	result := qe.requestHooks.ProcessRequestHooks(p, request)
	var isPaused bool
	err := responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		for _, extension := range result.Extensions {
			rb.SendExtensionData(extension)
		}
		if result.Err != nil {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return result.Err
		} else if !result.IsValidated {
			rb.FinishWithError(graphsync.RequestRejected)
			return errInvalidRequest
		} else if result.IsPaused {
			rb.PauseRequest()
			isPaused = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}
	if err := processDedupByKey(request, responseStream); err != nil {
		return nil, nil, false, err
	}
	if err := processDoNoSendCids(request, responseStream); err != nil {
		return nil, nil, false, err
	}
	if err := processDoNotSendFirstBlocks(request, responseStream); err != nil {
		return nil, nil, false, err
	}
	rootLink := cidlink.Link{Cid: request.Root()}
	linkSystem := result.CustomLinkSystem
	if linkSystem.StorageReadOpener == nil {
		linkSystem = qe.linkSystem
	}
	var budget *traversal.Budget
	if qe.maxLinksPerRequest > 0 {
		budget = &traversal.Budget{
			NodeBudget: math.MaxInt64,
			LinkBudget: int64(qe.maxLinksPerRequest),
		}
	}
	traverser := ipldutil.TraversalBuilder{
		Root:          rootLink,
		Selector:      request.Selector(),
		LinkSystem:    linkSystem,
		Chooser:       result.CustomChooser,
		Budget:        budget,
		PanicCallback: qe.panicCallback,
		Visitor: func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error {
			if vr != traversal.VisitReason_SelectionMatch {
				return nil
			}
			if lbn, ok := n.(datamodel.LargeBytesNode); ok {
				s, err := lbn.AsLargeBytes()
				if err != nil {
					log.Warnf("error %s in AsLargeBytes at path %s", err.Error(), p.Path)
				}
				_, err = io.Copy(io.Discard, s)
				if err != nil {
					log.Warnf("error %s reading bytes from reader at path %s", err.Error(), p.Path)
				}
			}
			return nil
		},
	}.Start(ctx)

	return linkSystem.StorageReadOpener, traverser, isPaused, nil
}

func processDedupByKey(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	dedupData, has := request.Extension(graphsync.ExtensionDeDupByKey)
	if !has {
		return nil
	}
	key, err := dedupkey.DecodeDedupKey(dedupData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return nil
		})
		return err
	}
	responseStream.DedupKey(key)
	return nil
}

func processDoNoSendCids(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	if !has {
		return nil
	}
	cidSet, err := cidset.DecodeCidSet(doNotSendCidsData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return nil
		})
		return err
	}
	links := make([]ipld.Link, 0, cidSet.Len())
	err = cidSet.ForEach(func(c cid.Cid) error {
		links = append(links, cidlink.Link{Cid: c})
		return nil
	})
	if err != nil {
		return err
	}
	responseStream.IgnoreBlocks(links)
	return nil
}

func processDoNotSendFirstBlocks(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	doNotSendFirstBlocksData, has := request.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
	if !has {
		return nil
	}
	skipCount, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return nil
		})
		return err
	}
	responseStream.SkipFirstBlocks(skipCount)
	return nil
}
