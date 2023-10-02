package impl

import (
	"github.com/Shopify/sarama"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"reflect"
	"testing"
	"time"
)

func TestSaramaMapper_ToCoreHeaders(t *testing.T) {
	type args struct {
		headers []sarama.RecordHeader
	}
	tests := []struct {
		name string
		args args
		want []core.MessageHeader
	}{
		{
			name: "Give full header Should returns correctly",
			args: args{
				headers: []sarama.RecordHeader{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			want: []core.MessageHeader{
				{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		},
		{
			name: "Give nil headers Should returns nil",
			args: args{
				headers: nil,
			},
			want: nil,
		},
		{
			name: "Give empty headers Should returns empty",
			args: args{
				headers: []sarama.RecordHeader{},
			},
			want: []core.MessageHeader{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := SaramaMapper{}
			if got := p.ToCoreHeaders(tt.args.headers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToCoreHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaramaMapper_PtrToCoreHeaders(t *testing.T) {
	type args struct {
		headers []*sarama.RecordHeader
	}
	tests := []struct {
		name string
		args args
		want []core.MessageHeader
	}{
		{
			name: "Give full header Should returns correctly",
			args: args{
				headers: []*sarama.RecordHeader{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			want: []core.MessageHeader{
				{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		},
		{
			name: "Give nil headers Should returns nil",
			args: args{
				headers: nil,
			},
			want: nil,
		},
		{
			name: "Give empty headers Should returns empty",
			args: args{
				headers: []*sarama.RecordHeader{},
			},
			want: []core.MessageHeader{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := SaramaMapper{}
			if got := p.PtrToCoreHeaders(tt.args.headers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PtrToCoreHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaramaMapper_ToSaramaHeaders(t *testing.T) {
	type args struct {
		headers []core.MessageHeader
	}
	tests := []struct {
		name string
		args args
		want []sarama.RecordHeader
	}{
		{
			name: "Give full header Should returns correctly",
			args: args{
				headers: []core.MessageHeader{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			want: []sarama.RecordHeader{
				{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		}, {
			name: "Give nil headers Should returns nil",
			args: args{
				headers: nil,
			},
			want: nil,
		},
		{
			name: "Give empty headers Should returns empty",
			args: args{
				headers: []core.MessageHeader{},
			},
			want: []sarama.RecordHeader{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := SaramaMapper{}
			if got := p.ToSaramaHeaders(tt.args.headers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToSaramaHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaramaMapper_ToCoreMessage(t *testing.T) {
	type args struct {
		msg *sarama.ProducerMessage
	}
	tests := []struct {
		name string
		args args
		want *core.Message
	}{
		{
			name: "Give full message Should returns correctly",
			args: args{msg: &sarama.ProducerMessage{
				Topic: "topic1",
				Key:   sarama.StringEncoder("key1"),
				Value: sarama.StringEncoder("value1"),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("h1"),
						Value: []byte("hv1"),
					},
				},
				Metadata:  "metadata-test",
				Offset:    10,
				Partition: 1,
				Timestamp: time.Unix(1663493407, 0),
			}},
			want: &core.Message{
				Topic: "topic1",
				Key:   []byte("key1"),
				Value: []byte("value1"),
				Headers: []core.MessageHeader{
					{
						Key:   []byte("h1"),
						Value: []byte("hv1"),
					},
				},
				Metadata:  "metadata-test",
				Offset:    10,
				Partition: 1,
				Timestamp: time.Unix(1663493407, 0),
			},
		},
		{
			name: "Give message without key Should returns key nil",
			args: args{msg: &sarama.ProducerMessage{
				Topic: "topic1",
				Key:   nil,
			}},
			want: &core.Message{
				Topic: "topic1",
				Key:   nil,
			},
		},
		{
			name: "Give message with empty key Should returns empty key",
			args: args{msg: &sarama.ProducerMessage{
				Topic: "topic1",
				Key:   sarama.StringEncoder(""),
			}},
			want: &core.Message{
				Topic: "topic1",
				Key:   []byte{},
			},
		},
		{
			name: "Give message without value Should returns value nil",
			args: args{msg: &sarama.ProducerMessage{
				Topic: "topic1",
				Value: nil,
			}},
			want: &core.Message{
				Topic: "topic1",
				Value: nil,
			},
		},
		{
			name: "Give message with empty value Should returns empty value",
			args: args{msg: &sarama.ProducerMessage{
				Topic: "topic1",
				Value: sarama.StringEncoder(""),
			}},
			want: &core.Message{
				Topic: "topic1",
				Value: []byte{},
			},
		},
		{
			name: "Give message without headers Should returns headers nil",
			args: args{msg: &sarama.ProducerMessage{
				Topic:   "topic1",
				Headers: nil,
			}},
			want: &core.Message{
				Topic:   "topic1",
				Headers: nil,
			},
		},
		{
			name: "Give message with empty headers Should returns empty headers",
			args: args{msg: &sarama.ProducerMessage{
				Topic:   "topic1",
				Headers: []sarama.RecordHeader{},
			}},
			want: &core.Message{
				Topic:   "topic1",
				Headers: []core.MessageHeader{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := SaramaMapper{}
			if got := p.ToCoreMessage(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToCoreMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaramaMapper_ToCoreConsumerMessage(t *testing.T) {
	type args struct {
		msg *sarama.ConsumerMessage
	}
	tests := []struct {
		name string
		args args
		want *core.ConsumerMessage
	}{
		{
			name: "Give full message Should returns correctly",
			args: args{msg: &sarama.ConsumerMessage{
				Topic: "topic1",
				Key:   []byte("key1"),
				Value: []byte("value1"),
				Headers: []*sarama.RecordHeader{
					{
						Key:   []byte("h1"),
						Value: []byte("hv1"),
					},
				},
				Offset:    10,
				Partition: 1,
				Timestamp: time.Unix(1663493407, 0),
			}},
			want: &core.ConsumerMessage{
				Topic: "topic1",
				Key:   []byte("key1"),
				Value: []byte("value1"),
				Headers: []core.MessageHeader{
					{
						Key:   []byte("h1"),
						Value: []byte("hv1"),
					},
				},
				Offset:    10,
				Partition: 1,
				Timestamp: time.Unix(1663493407, 0),
			},
		},
		{
			name: "Give message without key Should returns key nil",
			args: args{msg: &sarama.ConsumerMessage{
				Topic: "topic1",
				Key:   nil,
			}},
			want: &core.ConsumerMessage{
				Topic: "topic1",
				Key:   nil,
			},
		},
		{
			name: "Give message with empty key Should returns empty key",
			args: args{msg: &sarama.ConsumerMessage{
				Topic: "topic1",
				Key:   []byte{},
			}},
			want: &core.ConsumerMessage{
				Topic: "topic1",
				Key:   []byte{},
			},
		},
		{
			name: "Give message without value Should returns value nil",
			args: args{msg: &sarama.ConsumerMessage{
				Topic: "topic1",
				Value: nil,
			}},
			want: &core.ConsumerMessage{
				Topic: "topic1",
				Value: nil,
			},
		},
		{
			name: "Give message with empty value Should returns empty value",
			args: args{msg: &sarama.ConsumerMessage{
				Topic: "topic1",
				Value: []byte{},
			}},
			want: &core.ConsumerMessage{
				Topic: "topic1",
				Value: []byte{},
			},
		},
		{
			name: "Give message without headers Should returns headers nil",
			args: args{msg: &sarama.ConsumerMessage{
				Topic:   "topic1",
				Headers: nil,
			}},
			want: &core.ConsumerMessage{
				Topic:   "topic1",
				Headers: nil,
			},
		},
		{
			name: "Give message with empty headers Should returns empty headers",
			args: args{msg: &sarama.ConsumerMessage{
				Topic:   "topic1",
				Headers: []*sarama.RecordHeader{},
			}},
			want: &core.ConsumerMessage{
				Topic:   "topic1",
				Headers: []core.MessageHeader{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := SaramaMapper{}
			if got := p.ToCoreConsumerMessage(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToCoreConsumerMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}
