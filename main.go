package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("aws-region", "eu-west-1")
	viper.SetDefault("log-group", "gtfol")
	viper.SetDefault("log-stream", fmt.Sprintf("%s-%s", "gtfol", time.Now().String()))
	viper.SetDefault("cursor-file", "/gtfol/cursor")
	viper.SetDefault("journalctl", "journalctl")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.SetEnvPrefix("GTFOL")
	viper.AutomaticEnv()
}

// logBatch is many lines
type logBatch []jdEntry

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cursor := []byte{}
	var cursorStr *string
	cf, err := os.OpenFile(viper.GetString("cursor-file"), os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		log.Fatalf("Could not open cursor file: %s", err.Error())
	}
	cf.Read(cursor)
	if len(cursor) > 0 {
		c := string(cursor)
		cursorStr = &c
	}

	cursors := make(chan string)
	entries := make(chan jdEntry)
	batches := make(chan logBatch, 100)

	go reader(ctx, entries, cursors, cursorStr)
	go curser(ctx, cursors, cf)
	go batcher(ctx, entries, batches)
	go dispatcher(ctx, batches)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	for sig := range sigc {
		log.Printf("stopping with %v", sig)
		cancel()
		break
	}
}

func reader(ctx context.Context, entries chan jdEntry, cursors chan string, cursor *string) {
	cmd := exec.Command(viper.GetString("journalctl"), "--output", "json-sse", "--follow")
	if cursor != nil {
		cmd.Args = append(cmd.Args, "--cursor", *cursor)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Could not run journalctl: %s", err.Error())
	}
	scanner := bufio.NewScanner(stdout)
	err = cmd.Start()
	if err != nil {
		log.Fatalf("Could not run journalctl: %s", err.Error())
	}

	entry := jdEntry{}
	for scanner.Scan() {
		line := scanner.Text()

		err = json.Unmarshal([]byte(strings.Replace(line, "data:", "", 1)), &entry)
		if err != nil {
			log.Printf("failed unmarshal of log line: \"%s\"\n", line)
			continue
		}

		cursors <- entry.Cursor
		entries <- entry
	}
}

func curser(ctx context.Context, cursors chan string, cf *os.File) {
	for {
		select {
		case cursor := <-cursors:
			cf.Seek(0, 0)
			cf.WriteString(cursor)
		case <-ctx.Done():
			return
		}
	}
}

func batcher(ctx context.Context, entries chan jdEntry, batches chan logBatch) {
	batch := logBatch{}
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case e := <-entries:
			batch = append(batch, e)
			if len(batch) > 100 {
				batches <- batch
				batch = logBatch{}
			}
		case <-ticker.C:
			batches <- batch
			batch = logBatch{}
		case <-ctx.Done():
			return
		}
	}
}

func dispatcher(ctx context.Context, batches chan logBatch) {
	for {
		select {
		case b := <-batches:
			log.Println(len(b))
		case <-ctx.Done():
			return
		}
	}
}
