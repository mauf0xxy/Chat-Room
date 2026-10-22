// Package common holds the RPC contract shared by the chat client and server.
package common

import "time"

// Kind distinguishes the different things a client can receive.
type Kind int

const (
	KindChat     Kind = iota // a message written by another user
	KindNotice               // join / leave / timeout notifications
	KindShutdown             // the server is going away
)

// Message is the unit delivered to clients.
type Message struct {
	Kind Kind
	From string
	Text string
	Time time.Time
}

// ---- RPC arguments / replies -------------------------------------------

type Empty struct{}

type JoinArgs struct {
	Username string
}

type JoinReply struct {
	Username string   // canonical (trimmed) username
	Token    string   // secret proving ownership of the session
	Users    []string // users already in the room
}

// SessionArgs identifies a joined client on every later call.
type SessionArgs struct {
	Username string
	Token    string
}

type SendArgs struct {
	Session SessionArgs
	Text    string
}

type PollReply struct {
	Messages []Message
}

type UsersReply struct {
	Users []string
}

// ---- Limits & well-known errors ----------------------------------------

const (
	MaxNameLen = 20
	MaxTextLen = 1000

	// ErrNoSession is returned when the server does not know the caller
	// (never joined, already left, or was timed out as unreachable).
	ErrNoSession = "session not found or expired"
)
