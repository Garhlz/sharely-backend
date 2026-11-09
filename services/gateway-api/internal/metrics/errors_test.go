package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestResponseErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		code  int
		class string
	}{{40102, "business"}, {40001, "business"}, {50001, "server"}, {50004, "server"}} {
		before := testutil.ToFloat64(responseErrors.WithLabelValues(tc.class))
		ObserveResponseError(tc.code)
		if got := testutil.ToFloat64(responseErrors.WithLabelValues(tc.class)); got != before+1 {
			t.Fatalf("code %d: got %v", tc.code, got)
		}
	}
}
