package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type fakeAck struct {
	tag               uint64
	multiple, requeue bool
	action            string
	err               error
}

func (a *fakeAck) Ack(tag uint64, multiple bool) error {
	a.tag, a.multiple, a.action = tag, multiple, "ack"
	return a.err
}
func (a *fakeAck) Nack(tag uint64, multiple, requeue bool) error {
	a.tag, a.multiple, a.requeue, a.action = tag, multiple, requeue, "nack"
	return a.err
}
func (a *fakeAck) Reject(tag uint64, requeue bool) error {
	a.tag, a.requeue, a.action = tag, requeue, "reject"
	return a.err
}

func TestDeliverySettlementPreservesContract(t *testing.T) {
	empty := TrackDelivery(amqp.Delivery{}, "test")
	require.Error(t, empty.Ack(false))
	require.Error(t, empty.Nack(false, false))
	for _, tc := range []struct {
		result        string
		requeue, fail bool
		action        string
	}{
		{"acked", false, false, "ack"},
		{"requeue", true, false, "nack"},
		{"dead_letter", false, false, "nack"},
		{"dead_letter", false, false, "reject"},
		{"ack_error", false, true, "ack"},
	} {
		t.Run(tc.action+tc.result, func(t *testing.T) {
			ack := &fakeAck{}
			if tc.fail {
				ack.err = errors.New("closed")
			}
			before := testutil.ToFloat64(settled.WithLabelValues("test", tc.result))
			d := TrackDelivery(amqp.Delivery{DeliveryTag: 7, Acknowledger: ack}, "test")
			var err error
			switch tc.action {
			case "ack":
				err = d.Ack(true)
			case "nack":
				err = d.Nack(true, tc.requeue)
			case "reject":
				err = d.Reject(tc.requeue)
			}
			require.Equal(t, tc.fail, err != nil)
			require.EqualValues(t, 7, ack.tag)
			require.Equal(t, tc.action, ack.action)
			require.Equal(t, tc.requeue, ack.requeue)
			if tc.action != "reject" {
				require.True(t, ack.multiple)
			}
			require.Equal(t, before+1, testutil.ToFloat64(settled.WithLabelValues("test", tc.result)))
		})
	}
}
