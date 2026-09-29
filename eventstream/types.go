package eventstream

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var (
	ErrEventIDUUIDZero = errors.New("EventID uuid is zero")
	ErrEventIDUuidZero = ErrEventIDUUIDZero
	EventIDNil         = EventID(uuid.Nil)

	ErrUserIDUUIDZero = errors.New("UserID uuid is zero")
	ErrUserIDUuidZero = ErrUserIDUUIDZero
	UserIDNil         = UserID(uuid.Nil)
)

type EventID uuid.UUID

func NewEventID() EventID                          { return EventID(uuid.New()) }
func (t EventID) String() string                   { return uuid.UUID(t).String() }
func (t EventID) Value() (driver.Value, error)     { return t.String(), nil }
func (t *EventID) Scan(src any) error              { return (*uuid.UUID)(t).Scan(src) }
func (t EventID) MarshalText() ([]byte, error)     { return uuid.UUID(t).MarshalText() }
func (t *EventID) UnmarshalText(data []byte) error { return (*uuid.UUID)(t).UnmarshalText(data) }
func (t EventID) IsZero() bool                     { return t == EventIDNil }
func (t EventID) Matches(x any) bool {
	v, ok := x.(EventID)
	if !ok {
		return false
	}

	return t == v
}

func (t EventID) Validate() error {
	if t.IsZero() {
		return fmt.Errorf("validate: %w", ErrEventIDUUIDZero)
	}

	return nil
}

func (t EventID) AsPointer() *EventID {
	if t.IsZero() {
		return nil
	}

	return &t
}

type UserID uuid.UUID

func NewUserID() UserID                           { return UserID(uuid.New()) }
func (t UserID) String() string                   { return uuid.UUID(t).String() }
func (t UserID) Value() (driver.Value, error)     { return t.String(), nil }
func (t *UserID) Scan(src any) error              { return (*uuid.UUID)(t).Scan(src) }
func (t UserID) MarshalText() ([]byte, error)     { return uuid.UUID(t).MarshalText() }
func (t *UserID) UnmarshalText(data []byte) error { return (*uuid.UUID)(t).UnmarshalText(data) }
func (t UserID) IsZero() bool                     { return t == UserIDNil }
func (t UserID) Matches(x any) bool {
	v, ok := x.(UserID)
	if !ok {
		return false
	}

	return t == v
}

func (t UserID) Validate() error {
	if t.IsZero() {
		return fmt.Errorf("validate: %w", ErrUserIDUUIDZero)
	}

	return nil
}

func (t UserID) AsPointer() *UserID {
	if t.IsZero() {
		return nil
	}

	return &t
}

type TypeSet interface {
	EventID | UserID
}

func Parse[T TypeSet](s string) (T, error) {
	v, err := uuid.Parse(s)
	return T(v), err
}

func MustParse[T TypeSet](s string) T {
	return T(uuid.MustParse(s))
}

func ParseEventID(s string) (EventID, error) {
	return Parse[EventID](s)
}

func MustParseEventID(s string) EventID {
	return MustParse[EventID](s)
}

func ParseUserID(s string) (UserID, error) {
	return Parse[UserID](s)
}

func MustParseUserID(s string) UserID {
	return MustParse[UserID](s)
}
