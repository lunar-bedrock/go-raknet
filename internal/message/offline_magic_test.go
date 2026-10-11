package message

import (
	"net/netip"
	"testing"
)

func TestOfflineReplyMagicValidation(t *testing.T) {
	for _, security := range []bool{false, true} {
		first := &OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: security, Cookie: 7, MTU: 1200}
		b, _ := first.MarshalBinary()
		decoded := &OpenConnectionReply1{}
		if err := decoded.UnmarshalBinary(b[1:]); err != nil {
			t.Fatal(err)
		}
		if decoded.ServerGUID != first.ServerGUID || decoded.MTU != first.MTU || decoded.ServerHasSecurity != security || (security && decoded.Cookie != first.Cookie) {
			t.Fatalf("Reply 1 round trip: %+v", decoded)
		}
		b[1] ^= 1
		if err := decoded.UnmarshalBinary(b[1:]); err == nil {
			t.Fatal("Reply 1 accepted wrong magic")
		}
	}
	for _, address := range []string{"192.0.2.1:10000", "[2001:db8::1]:10000"} {
		second := &OpenConnectionReply2{ServerGUID: 1, ClientAddress: netip.MustParseAddrPort(address), MTU: 1400, DoSecurity: true}
		b, _ := second.MarshalBinary()
		decoded := &OpenConnectionReply2{}
		if err := decoded.UnmarshalBinary(b[1:]); err != nil {
			t.Fatal(err)
		}
		if *decoded != *second {
			t.Fatalf("Reply 2 round trip: %+v", decoded)
		}
		b[1] ^= 1
		if err := decoded.UnmarshalBinary(b[1:]); err == nil {
			t.Fatal("Reply 2 accepted wrong magic")
		}
	}
}
