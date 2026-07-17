package raknet

import (
	"slices"
	"testing"
)

func TestMTUSizesFor(t *testing.T) {
	tests := []struct {
		name   string
		maxMTU uint16
		want   []uint16
	}{
		{name: "default", want: []uint16{1200, 1492, 576}},
		{name: "supported maximum", maxMTU: 1492, want: []uint16{1200, 1492, 576}},
		{name: "between preferred and maximum", maxMTU: 1400, want: []uint16{1400, 1200, 576}},
		{name: "preferred", maxMTU: 1200, want: []uint16{1200, 576}},
		{name: "below preferred", maxMTU: 1000, want: []uint16{1000, 576}},
		{name: "minimum", maxMTU: 576, want: []uint16{576}},
		{name: "below minimum", maxMTU: 400, want: []uint16{576}},
		{name: "above supported maximum", maxMTU: 1600, want: []uint16{1200, 1492, 576}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mtuSizesFor(tt.maxMTU); !slices.Equal(got, tt.want) {
				t.Fatalf("mtuSizesFor(%d) = %v, want %v", tt.maxMTU, got, tt.want)
			}
		})
	}
}
