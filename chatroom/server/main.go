// Command server runs the chat room RPC server.
//
// Delivery model: POLLING. Each client repeatedly calls Chat.Poll. The call
// "long-polls" (blocks up to pollWait for something to arrive), so messages
// show up almost instantly without hammering the server. Because every
// connected client polls continuously, Poll doubles as a heartbeat: a client
// that stops polling for clientTimeout is treated as disconnected.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"chatroom/common"
)

const (
	pollWait      = 2 * time.Second  // max time one Poll call blocks
	clientTimeout = 10 * time.Second // silence after which a client is dropped
	reapInterval  = 2 * time.Second
)

// session is the server-side state for one connected user.
// All fields are protected by ChatService.mu.
type session struct {
	name     string
	token    string
	inbox    []common.Message
	lastSeen time.Time
	wake     chan struct{} // buffered(1); signals a blocked Poll
}

// ChatService is registered with net/rpc under the name "Chat".
type ChatService struct {
	mu      sync.Mutex
	users   map[string]*session // key: lower-cased username
	closing bool
}

func NewChatService() *ChatService {
	return &ChatService{users: make(map[string]*session)}
}

// ---------------------------------------------------------------------------
// RPC methods
// ---------------------------------------------------------------------------

// Join registers a unique username and announces it to everyone else.
func (s *ChatService) Join(args common.JoinArgs, reply *common.JoinReply) error {
	name := strings.TrimSpace(args.Username)
	if err := validateName(name); err != nil {
		return err
	}
	key := strings.ToLower(name)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return errors.New("server is shutting down")
	}
	if _, taken := s.users[key]; taken {
		return fmt.Errorf("username %q is already taken", name)
	}

	u := &session{
		name:     name,
		token:    newToken(),
		lastSeen: time.Now(),
		wake:     make(chan struct{}, 1),
	}
	reply.Username = name
	reply.Token = u.token
	reply.Users = s.namesLocked() // users present *before* this one joins

	s.users[key] = u
	s.deliverLocked(notice(name+" joined the chat"), key)
	log.Printf("join: %s (%d online)", name, len(s.users))
	return nil
}

// Leave removes the caller and tells everyone else.
func (s *ChatService) Leave(args common.SessionArgs, _ *common.Empty) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.authLocked(args); err != nil {
		return err
	}
	s.removeLocked(strings.ToLower(args.Username), "left the chat")
	return nil
}

// Send broadcasts a message to every user except the sender.
func (s *ChatService) Send(args common.SendArgs, _ *common.Empty) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return errors.New("empty message")
	}
	if utf8.RuneCountInString(text) > common.MaxTextLen {
		return fmt.Errorf("message too long (max %d characters)", common.MaxTextLen)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.authLocked(args.Session)
	if err != nil {
		return err
	}
	if s.closing {
		return errors.New("server is shutting down")
	}
	s.deliverLocked(common.Message{
		Kind: common.KindChat, From: u.name, Text: text, Time: time.Now(),
	}, strings.ToLower(u.name))
	return nil
}

// ListUsers returns the names of all connected users.
func (s *ChatService) ListUsers(args common.SessionArgs, reply *common.UsersReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.authLocked(args); err != nil {
		return err
	}
	reply.Users = s.namesLocked()
	return nil
}

// Poll returns all pending messages for the caller. If none are waiting it
// blocks for up to pollWait before returning an empty batch.
func (s *ChatService) Poll(args common.SessionArgs, reply *common.PollReply) error {
	s.mu.Lock()
	u, err := s.authLocked(args)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	if len(u.inbox) == 0 && !s.closing {
		wake := u.wake
		s.mu.Unlock() // never hold the lock while waiting
		select {
		case <-wake:
		case <-time.After(pollWait):
		}
		s.mu.Lock()
		if u, err = s.authLocked(args); err != nil { // may have left meanwhile
			s.mu.Unlock()
			return err
		}
	}

	reply.Messages = u.inbox
	u.inbox = nil
	s.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers (all *Locked helpers require s.mu to be held)
// ---------------------------------------------------------------------------

// authLocked checks the caller's token and refreshes its heartbeat.
func (s *ChatService) authLocked(a common.SessionArgs) (*session, error) {
	u, ok := s.users[strings.ToLower(a.Username)]
	if !ok || u.token != a.Token {
		return nil, errors.New(common.ErrNoSession)
	}
	u.lastSeen = time.Now()
	return u, nil
}

// deliverLocked appends m to every inbox except exceptKey's and wakes pollers.
func (s *ChatService) deliverLocked(m common.Message, exceptKey string) {
	for key, u := range s.users {
		if key == exceptKey {
			continue
		}
		u.inbox = append(u.inbox, m)
		select {
		case u.wake <- struct{}{}:
		default: // already signalled
		}
	}
}

func (s *ChatService) removeLocked(key, how string) {
	u, ok := s.users[key]
	if !ok {
		return
	}
	delete(s.users, key)
	s.deliverLocked(notice(u.name+" "+how), "")
	log.Printf("%s: %s (%d online)", how, u.name, len(s.users))
}

func (s *ChatService) namesLocked() []string {
	names := make([]string, 0, len(s.users))
	for _, u := range s.users {
		names = append(names, u.name)
	}
	sort.Strings(names)
	return names
}

// reaper drops clients that stopped polling (crash, network loss, kill -9).
func (s *ChatService) reaper(ctx context.Context) {
	t := time.NewTicker(reapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			for key, u := range s.users {
				if time.Since(u.lastSeen) > clientTimeout {
					s.removeLocked(key, "lost connection (timed out)")
				}
			}
			s.mu.Unlock()
		}
	}
}

// beginShutdown stops accepting new work and tells every client to disconnect.
func (s *ChatService) beginShutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	s.deliverLocked(common.Message{
		Kind: common.KindShutdown, Text: "Server is shutting down", Time: time.Now(),
	}, "")
}

// waitDrained waits (bounded) until every client has fetched its inbox.
func (s *ChatService) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		pending := false
		for _, u := range s.users {
			if len(u.inbox) > 0 {
				pending = true
				break
			}
		}
		s.mu.Unlock()
		if !pending {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func notice(text string) common.Message {
	return common.Message{Kind: common.KindNotice, Text: text, Time: time.Now()}
}

func validateName(name string) error {
	n := utf8.RuneCountInString(name)
	if n == 0 {
		return errors.New("username cannot be empty")
	}
	if n > common.MaxNameLen {
		return fmt.Errorf("username too long (max %d characters)", common.MaxNameLen)
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("username cannot contain spaces or control characters")
		}
	}
	return nil
}

func newToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------

func main() {
	addr := flag.String("addr", ":1234", "address to listen on")
	flag.Parse()

	svc := NewChatService()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Chat", svc); err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s (Ctrl+C to stop)", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go svc.reaper(ctx)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("accept: %v", err)
				continue
			}
			go srv.ServeConn(conn) // one goroutine per client connection
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	svc.beginShutdown()
	svc.waitDrained(2 * time.Second)
	time.Sleep(200 * time.Millisecond) // let the last replies flush
	ln.Close()
	log.Println("bye")
}
