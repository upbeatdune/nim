package bundler

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/weeaa/jito-go/clients/searcher_client"
	jito_pb "github.com/weeaa/jito-go/pb"
)

var (
	blockEngineURL = flag.String("jito-block-engine", envOrDefault("JITO_BLOCK_ENGINE_URL", "mainnet.block-engine.jito.wtf:443"), "Jito block engine grpc endpoint")
	keypairPath    = flag.String("jito-keypair", envOrDefault("JITO_KEYPAIR_PATH", ""), "path to the searcher identity keypair (solana-keygen json)")
	tipLamports    = flag.Uint64("jito-tip-lamports", 15000, "lamports attached to every bundle as the jito tip")
	maxBundleSize  = flag.Int("jito-bundle-size", 5, "max transactions per bundle, tip tx included")
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// BundleSubmitter wraps a Jito searcher client and adds the tip leg to
// every bundle it sends. Stats are aggregated from the result stream.
type BundleSubmitter struct {
	sent   uint64
	landed uint64
	missed uint64

	client *searcher_client.Client
	payer  solana.PrivateKey
}

// NewBundleSubmitter dials the block engine, authenticates the searcher
// identity over the grpc pubkey challenge/response flow, and starts the
// bundle result stream in the background.
func NewBundleSubmitter(ctx context.Context, rpcClient, jitoRPC *rpc.Client) (*BundleSubmitter, error) {
	if *keypairPath == "" {
		return nil, fmt.Errorf("bundler: -jito-keypair (or JITO_KEYPAIR_PATH) not set")
	}

	identity, err := solana.PrivateKeyFromSolanaKeygenFile(*keypairPath)
	if err != nil {
		return nil, fmt.Errorf("bundler: loading searcher identity from %s: %w", *keypairPath, err)
	}

	client, err := searcher_client.New(ctx, *blockEngineURL, jitoRPC, rpcClient, identity, &tls.Config{})
	if err != nil {
		return nil, fmt.Errorf("bundler: dialing block engine %s: %w", *blockEngineURL, err)
	}

	s := &BundleSubmitter{
		client: client,
		payer:  identity,
	}

	go s.trackLandRate(ctx)

	return s, nil
}

func (s *BundleSubmitter) trackLandRate(ctx context.Context) {
	sub, err := s.client.NewBundleSubscriptionResults()
	if err != nil {
		log.Printf("bundler: bundle result stream unavailable, land rate tracking disabled: %v", err)
		return
	}

	for {
		res, err := sub.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("bundler: bundle result stream closed, land rate tracking stopped: %v", err)
			return
		}

		switch res.GetResult().(type) {
		case *jito_pb.BundleResult_Finalized:
			atomic.AddUint64(&s.landed, 1)
		case *jito_pb.BundleResult_Rejected, *jito_pb.BundleResult_Dropped:
			atomic.AddUint64(&s.missed, 1)
		}
	}
}

// LandRate returns the fraction of finalized bundles among everything the
// stream has resolved so far (finalized vs rejected/dropped). It returns 0
// until the first bundle resolves one way or the other.
func (s *BundleSubmitter) LandRate() float64 {
	landed := atomic.LoadUint64(&s.landed)
	missed := atomic.LoadUint64(&s.missed)
	total := landed + missed
	if total == 0 {
		return 0
	}
	return float64(landed) / float64(total)
}

// SubmitBundle appends a fresh tip transaction paid from the searcher
// identity and sends the resulting bundle. It returns the bundle
// uuid the block engine assigned, which callers can correlate against the
// result stream if they need per-bundle status rather than the aggregate
// land rate.
func (s *BundleSubmitter) SubmitBundle(ctx context.Context, txs []*solana.Transaction) (string, error) {
	if len(txs)+1 > *maxBundleSize {
		return "", fmt.Errorf("bundler: %d txs plus tip exceeds configured max of %d", len(txs), *maxBundleSize)
	}

	tipAccount, err := s.client.GetRandomTipAccount()
	if err != nil {
		return "", fmt.Errorf("bundler: fetching tip account: %w", err)
	}

	bh, err := s.client.RpcConn.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return "", fmt.Errorf("bundler: fetching blockhash for tip tx: %w", err)
	}

	tipTx, err := solana.NewTransaction(
		[]solana.Instruction{
			system.NewTransferInstruction(*tipLamports, s.payer.PublicKey(), solana.MustPublicKeyFromBase58(tipAccount)).Build(),
		},
		bh.Value.Blockhash,
		solana.TransactionPayer(s.payer.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("bundler: building tip tx: %w", err)
	}

	if _, err = tipTx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(s.payer.PublicKey()) {
			return &s.payer
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("bundler: signing tip tx: %w", err)
	}

	resp, err := s.client.SendBundle(append(txs, tipTx))
	if err != nil {
		return "", fmt.Errorf("bundler: submitting bundle: %w", err)
	}

	atomic.AddUint64(&s.sent, 1)

	return resp.Uuid, nil
}
