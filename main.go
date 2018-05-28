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

	"github.com/cdemers/journald2graylog/journald"
	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("aws-region", "eu-west-1")
	viper.SetDefault("log-group", "gtfol")
	viper.SetDefault("log-stream", fmt.Sprintf("%s-%s", "gtfol", time.Now().String()))
	viper.SetDefault("cursor-file", "/gtfol/cursor")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.SetEnvPrefix("GTFOL")
	viper.AutomaticEnv()
}

// jdlog is shorted than "journald.JournaldJSONLogEntry"
type jdlog journald.JournaldJSONLogEntry

// logBatch is many lines
type logBatch []jdlog

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
	entries := make(chan jdlog)
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

func reader(ctx context.Context, entries chan jdlog, cursors chan string, cursor *string) {
	cmd := exec.Command("journalctl", "--output", "json-sse", "--follow", "--show-cursor")
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

	entry := jdlog{}
	for {
		line := scanner.Bytes()

		// -- cursor: s=0639...
		if string(line[0:9]) == "-- cursor" {
			cursors <- string(line[13:])
		}

		err = json.Unmarshal(line, &entry)
		if err != nil {
			log.Printf("failed unmarshal of log line: \"%s\"\n", line)
			continue
		}

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

func batcher(ctx context.Context, entries chan jdlog, batches chan logBatch) {
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