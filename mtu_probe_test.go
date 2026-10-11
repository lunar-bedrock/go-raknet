package raknet

import (
	"context"
	"fmt"
	"reflect"
	"syscall"
	"testing"
)

func TestMTUProbesAdvanceImmediatelyWhenDatagramTooLarge(t *testing.T) {
	var sent []uint16
	waits := 0
	sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(size uint16) error {
		sent = append(sent, size)
		if size == 1492 {
			return fmt.Errorf("probe write: %w", syscall.EMSGSIZE)
		}
		return nil
	}, func() bool { waits++; return false })
	if !reflect.DeepEqual(sent, []uint16{1492, 1200}) || waits != 1 {
		t.Fatalf("probes=%v cadence waits=%d, want immediate lower probe before waiting", sent, waits)
	}
}

func TestMTUProbesPreserveCadenceForOtherWriteResults(t *testing.T) {
	for _, result := range []error{nil, syscall.EHOSTUNREACH} {
		var sent []uint16
		waits := 0
		sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(size uint16) error { sent = append(sent, size); return result }, func() bool { waits++; return true })
		want := []uint16{1492, 1492, 1492, 1492, 1200, 1200, 1200, 1200, 576, 576, 576, 576}
		if !reflect.DeepEqual(sent, want) || waits != len(want) {
			t.Fatalf("result=%v probes=%v waits=%d", result, sent, waits)
		}
	}
}

func TestMTUProbesStopOnCancellationOrExhaustedSizes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sendMTUProbes(ctx, []uint16{1492}, func(uint16) error { t.Fatal("send after cancellation"); return nil }, func() bool { t.Fatal("wait after cancellation"); return true })
	sends := 0
	sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(uint16) error { sends++; return syscall.EMSGSIZE }, func() bool { t.Fatal("wait after rejected size"); return true })
	if sends != 3 {
		t.Fatalf("rejected-size sends=%d", sends)
	}
}
