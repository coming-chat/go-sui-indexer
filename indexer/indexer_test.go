package indexer

import (
	"context"
	"github.com/coming-chat/go-sui/client"
	"net/http"
	"testing"
	"time"
)

func TestFindNumber(t *testing.T) {
	hc := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:    20,
			IdleConnTimeout: 20 * time.Second,
		},
		Timeout: 15 * time.Second,
	}
	ct, _ := client.DialWithClient("https://fullnode.devnet.sui.io", hc)
	for i := 10000000; i < 24941756; i += 1000 {
		t.Logf("current transaction number ======> %d", i)
		var (
			err    error
			digest []string
		)
		for {
			digest, err = ct.GetTransactionsInRange(context.Background(), uint64(i), uint64(i+1000))
			if err != nil {
				t.Log(err)
				continue
			}
			break
		}

		for j, v := range digest {
			if v == "6AqFySL4hYYSaEssY7YJS2qGd11tUfhzNm1Tq3fiJVxf" {
				t.Log(i + j)
				break
			}
		}
	}

}
