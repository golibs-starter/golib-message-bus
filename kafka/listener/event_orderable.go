package listener

type EventOrderable interface {
	OrderingKey() string
}
