package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/czqu/rmtt-go/client"
)

// Cross-language end-to-end probe: an rmtt-go v1.0.2 client against the Java
// rmtt server (rmtt-it E2eServerMain). It connects, pushes several messages
// upstream concurrently and verifies the server's echoes come back downstream
// concurrently, proving bidirectional interoperability. Exit 0 on success.
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go-client <port>")
		os.Exit(2)
	}
	addr := "tcp://127.0.0.1:" + os.Args[1]

	opts := client.NewClientOptions()
	opts.AddServer(addr)
	opts.SetCredential("go-e2e")
	opts.SetConnectTimeout(3 * time.Second)
	opts.AutoReconnect = false

	c := client.NewClient(opts)

	echo := make(chan []byte, 16)
	c.AddPayloadHandlerLast(func(_ client.Client, m client.Message) {
		echo <- m.Payload()
	})

	if t := c.Connect(); t.Wait() && t.Error() != nil {
		fmt.Fprintln(os.Stderr, "connect failed:", t.Error())
		os.Exit(1)
	}
	fmt.Println("GO_CONNECTED")

	// concurrent upstream pushes; the Java server echoes each one downstream
	const n = 5
	var wg sync.WaitGroup
	var pushErr error
	var errMu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if t := c.Push(fmt.Sprintf("ping-%d", i)); t.WaitTimeout(3*time.Second) && t.Error() != nil {
				errMu.Lock()
				pushErr = fmt.Errorf("push %d failed: %w", i, t.Error())
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if pushErr != nil {
		fmt.Fprintln(os.Stderr, pushErr)
		os.Exit(1)
	}

	// verify every echo arrives downstream
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		want[fmt.Sprintf("echo:ping-%d", i)] = true
	}
	for i := 0; i < n; i++ {
		select {
		case p := <-echo:
			delete(want, string(p))
		case <-time.After(5 * time.Second):
			fmt.Fprintf(os.Stderr, "timeout waiting for echo %d of %d\n", i, n)
			os.Exit(1)
		}
	}
	if len(want) != 0 {
		fmt.Fprintf(os.Stderr, "missing echoes: %v\n", want)
		os.Exit(1)
	}

	fmt.Printf("GO_ECHO_OK (%d concurrent echoes verified)\n", n)
	c.Disconnect(100)

	// Phase 2: regression guards for the blackbox report - a JWT-like credential well above
	// the old 23-byte cap (F2) and a 1 MiB frame in both directions above the old 8 KiB
	// cap (F1). The Java server must accept both like rmtt-go / rmtt-rust do.
	opts2 := client.NewClientOptions()
	opts2.AddServer(addr)
	opts2.SetCredential(strings.Repeat("j", 300))
	opts2.SetConnectTimeout(3 * time.Second)
	opts2.AutoReconnect = false
	c2 := client.NewClient(opts2)
	bigEcho := make(chan []byte, 4)
	c2.AddPayloadHandlerLast(func(_ client.Client, m client.Message) {
		bigEcho <- m.Payload()
	})
	if t := c2.Connect(); t.Wait() && t.Error() != nil {
		fmt.Fprintln(os.Stderr, "long-credential connect failed:", t.Error())
		os.Exit(1)
	}
	fmt.Println("GO_LONG_CRED_OK (300-byte credential accepted)")

	big := bytes.Repeat([]byte("0123456789abcdef"), 65536) // 1 MiB
	if t := c2.Push(string(big)); t.WaitTimeout(10*time.Second) && t.Error() != nil {
		fmt.Fprintln(os.Stderr, "1 MiB push failed:", t.Error())
		os.Exit(1)
	}
	select {
	case p := <-bigEcho:
		if !bytes.Equal(p, append([]byte("echo:"), big...)) {
			fmt.Fprintf(os.Stderr, "1 MiB echo mismatch: got %d bytes\n", len(p))
			os.Exit(1)
		}
	case <-time.After(10 * time.Second):
		fmt.Fprintln(os.Stderr, "timeout waiting for 1 MiB echo")
		os.Exit(1)
	}
	fmt.Println("GO_LARGE_ECHO_OK (1 MiB round trip)")
	c2.Disconnect(100)

	fmt.Println("GO_E2E_PASS")
	os.Exit(0)
}
