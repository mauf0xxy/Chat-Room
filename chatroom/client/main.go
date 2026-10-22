// Command client is the terminal chat client.
//
// Three goroutines cooperate:
//   - pollLoop   : repeatedly calls Chat.Poll and prints whatever arrives
//   - inputLoop  : reads stdin; sends messages and handles /commands
//   - main       : waits until shutdown() closes c.done
//
// Ctrl+C, Ctrl+D, /quit, server shutdown and connection loss all funnel into
// shutdown(), which runs exactly once.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/common"
)

const promptStr = "> "

// terminal serialises output so incoming messages don't tear each other and
// the input prompt is redrawn after each one.
type terminal struct{ mu sync.Mutex }

func (t *terminal) prompt() {
	t.mu.Lock()
	fmt.Print(promptStr)
	t.mu.Unlock()
}

// line prints s on its own line, clearing any half-drawn prompt first.
func (t *terminal) line(s string) {
	t.mu.Lock()
	fmt.Printf("\r\033[K%s\n%s", s, promptStr)
	t.mu.Unlock()
}

type chatClient struct {
	rpc  *rpc.Client
	sess common.SessionArgs
	ui   *terminal
	done chan struct{}
	once sync.Once
}

// shutdown disconnects exactly once. sendLeave=false is used when the server
// is already gone (or has told us to go away).
func (c *chatClient) shutdown(sendLeave bool) {
	c.once.Do(func() {
		if sendLeave {
			call := c.rpc.Go("Chat.Leave", c.sess, &common.Empty{}, nil)
			select {
			case <-call.Done:
			case <-time.After(2 * time.Second):
			}
		}
		close(c.done) // signal first so pollLoop treats the Close() error as expected
		c.rpc.Close()
	})
}

func (c *chatClient) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *chatClient) pollLoop() {
	for {
		var reply common.PollReply
		err := c.rpc.Call("Chat.Poll", c.sess, &reply)
		if c.isDone() {
			return
		}
		if err != nil {
			c.ui.line("*** Connection lost: " + err.Error())
			c.shutdown(false)
			return
		}
		serverGone := false
		for _, m := range reply.Messages {
			c.ui.line(format(m))
			if m.Kind == common.KindShutdown {
				serverGone = true
			}
		}
		if serverGone {
			c.shutdown(false)
			return
		}
	}
}

func (c *chatClient) inputLoop(in *bufio.Reader) {
	for {
		c.ui.prompt()
		text, err := in.ReadString('\n')
		if err != nil { // EOF (Ctrl+D) or closed stdin
			if err != io.EOF {
				c.ui.line("*** input error: " + err.Error())
			}
			c.shutdown(true)
			return
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		switch {
		case text == "/quit" || text == "/exit":
			c.shutdown(true)
			return
		case text == "/users":
			var r common.UsersReply
			if err := c.rpc.Call("Chat.ListUsers", c.sess, &r); err != nil {
				c.handleCallErr(err)
				continue
			}
			c.ui.line(fmt.Sprintf("*** Online (%d): %s", len(r.Users), strings.Join(r.Users, ", ")))
		case text == "/help":
			c.ui.line("*** Commands: /users  list connected users | /quit  leave | /help")
		case strings.HasPrefix(text, "/"):
			c.ui.line("*** Unknown command (try /help)")
		default:
			args := common.SendArgs{Session: c.sess, Text: text}
			if err := c.rpc.Call("Chat.Send", args, &common.Empty{}); err != nil {
				c.handleCallErr(err)
				continue
			}
		}
	}
}

func (c *chatClient) handleCallErr(err error) {
	if c.isDone() {
		return
	}
	c.ui.line("*** Error: " + err.Error())
	if _, isServerErr := err.(rpc.ServerError); !isServerErr || err.Error() == common.ErrNoSession {
		c.shutdown(false) // transport failure or our session is gone
	}
}

func format(m common.Message) string {
	ts := m.Time.Local().Format("15:04")
	if m.Kind == common.KindChat {
		return fmt.Sprintf("[%s] %s: %s", ts, m.From, m.Text)
	}
	return fmt.Sprintf("[%s] *** %s", ts, m.Text)
}

func main() {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (prompted if omitted)")
	flag.Parse()

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to server:", err)
		os.Exit(1)
	}
	in := bufio.NewReader(os.Stdin)

	// Join, retrying with a new name if it is rejected (e.g. duplicate).
	var joined common.JoinReply
	for {
		if *name == "" {
			fmt.Print("Choose a username: ")
			line, err := in.ReadString('\n')
			if err != nil {
				client.Close()
				return
			}
			*name = strings.TrimSpace(line)
		}
		err := client.Call("Chat.Join", common.JoinArgs{Username: *name}, &joined)
		if err == nil {
			break
		}
		if _, isServerErr := err.(rpc.ServerError); !isServerErr {
			fmt.Fprintln(os.Stderr, "connection error:", err)
			os.Exit(1)
		}
		fmt.Println("Cannot join:", err)
		*name = "" // ask again
	}

	c := &chatClient{
		rpc:  client,
		sess: common.SessionArgs{Username: joined.Username, Token: joined.Token},
		ui:   &terminal{},
		done: make(chan struct{}),
	}

	fmt.Printf("Welcome, %s! Online now: %s\n", joined.Username, orNobody(joined.Users))
	fmt.Println("Type a message and press Enter. /users, /help, /quit are available.")

	// Ctrl+C / SIGTERM -> leave politely.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		select {
		case <-ctx.Done():
			c.shutdown(true)
		case <-c.done:
		}
	}()

	go c.pollLoop()
	go c.inputLoop(in)

	<-c.done
	fmt.Print("\r\033[K")
	fmt.Println("Disconnected. Goodbye!")
}

func orNobody(users []string) string {
	if len(users) == 0 {
		return "(nobody else yet)"
	}
	return strings.Join(users, ", ")
}
