package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/rpc"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/weeaa/jito-go/clients/searcher_client"
)

const userAgent = "alertbot by @kasha_tar (personal use)"

var (
	tgToken   = os.Getenv("TG_TOKEN")
	chatID    int64
	jitoURL   = os.Getenv("JITO_URL")
	rpcURL    = os.Getenv("RPC_URL")
	bot       *tgbotapi.BotAPI
	client    *searcher_client.Client
	rpcCli    *rpc.Client
	key       solana.PrivateKey
	mu        sync.Mutex
	submitted = map[string]bool{}
	sent      int
	landed    int
	started   = time.Now()
)

const tipLamports = 10000

func send(text string) {
	if _, err := bot.Send(tgbotapi.NewMessage(chatID, text)); err != nil {
		log.Println("tg send failed:", err)
	}
}

func buildTipTx(ctx context.Context) (*solana.Transaction, error) {
	bh, err := rpcCli.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return nil, err
	}
	tipStr, err := client.GetRandomTipAccount()
	if err != nil {
		return nil, err
	}
	tip, err := solana.PublicKeyFromBase58(tipStr)
	if err != nil {
		return nil, err
	}
	ix := system.NewTransferInstruction(tipLamports, key.PublicKey(), tip).Build()
	tx, err := solana.NewTransaction([]solana.Instruction{ix}, bh.Value.Blockhash, solana.TransactionPayer(key.PublicKey()))
	if err != nil {
		return nil, err
	}
	_, err = tx.Sign(func(p solana.PublicKey) *solana.PrivateKey {
		if p.Equals(key.PublicKey()) {
			return &key
		}
		return nil
	})
	return tx, err
}

func ping() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := buildTipTx(ctx)
	if err != nil {
		send("build failed: " + err.Error())
		return
	}
	resp, err := client.BroadcastBundle([]*solana.Transaction{tx})
	if err != nil {
		send("broadcast failed: " + err.Error())
		return
	}
	mu.Lock()
	submitted[resp.Uuid] = true
	sent++
	mu.Unlock()
	log.Println("sent bundle", resp.Uuid)
	send("bundle sent: " + resp.Uuid)
}

func watchResults() {
	for {
		sub, err := client.NewBundleSubscriptionResults()
		if err != nil {
			log.Println("subscribe:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		for {
			res, err := sub.Recv()
			if err != nil {
				log.Println("recv:", err)
				break
			}
			id := res.GetBundleId()
			mu.Lock()
			ok := submitted[id]
			mu.Unlock()
			if !ok {
				continue
			}
			switch {
			case res.GetProcessed() != nil, res.GetFinalized() != nil:
				mu.Lock()
				landed++
				delete(submitted, id)
				mu.Unlock()
				log.Println("landed", id)
			case res.GetRejected() != nil:
				send(fmt.Sprintf("bundle %s rejected: %v", id, res.GetRejected()))
				mu.Lock()
				delete(submitted, id)
				mu.Unlock()
			case res.GetDropped() != nil:
				send(fmt.Sprintf("bundle %s dropped: %v", id, res.GetDropped()))
				mu.Lock()
				delete(submitted, id)
				mu.Unlock()
			}
		}
		time.Sleep(3 * time.Second)
	}
}

func stats() string {
	mu.Lock()
	defer mu.Unlock()
	rate := 0.0
	if sent > 0 {
		rate = float64(landed) / float64(sent) * 100
	}
	return fmt.Sprintf("up %s\nsent %d, landed %d, rate %.1f%%",
		time.Since(started).Round(time.Second), sent, landed, rate)
}

func main() {
	var err error
	chatID, err = strconv.ParseInt(os.Getenv("TG_CHAT_ID"), 10, 64)
	if err != nil || tgToken == "" || jitoURL == "" || rpcURL == "" {
		log.Fatal("need TG_TOKEN, TG_CHAT_ID, JITO_URL, RPC_URL, KEYPAIR")
	}
	key, err = solana.PrivateKeyFromSolanaKeygenFile(os.Getenv("KEYPAIR"))
	if err != nil {
		log.Fatal(err)
	}
	rpcCli = rpc.New(rpcURL)
	client, err = searcher_client.New(context.Background(), jitoURL, rpcCli, rpcCli, key, nil)
	if err != nil {
		log.Fatal("jito auth: ", err)
	}
	bot, err = tgbotapi.NewBotAPI(tgToken)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(userAgent, "running as", bot.Self.UserName, "key", key.PublicKey())
	go watchResults()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	for upd := range bot.GetUpdatesChan(u) {
		if upd.Message == nil || upd.Message.Chat.ID != chatID {
			continue
		}
		switch upd.Message.Command() {
		case "start":
			send(userAgent + "\n/ping sends a tip bundle\n/stats shows land rate")
		case "ping":
			go ping()
		case "stats":
			send(stats())
		}
	}
}
