package gocoverage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"runtime/coverage"
	"strings"
	"sync"
	"syscall"
	"time"
)

// LogFunc is the signature for a pluggable logger.
type LogFunc func(format string, args ...any)

var (
	logMu  sync.RWMutex
	logger LogFunc = func(format string, args ...any) { fmt.Printf(format, args...) }
)

// SetLogger sets the function used to emit coverage diagnostics. fn must be
// safe for concurrent use. If fn is nil the call is a no-op.
func SetLogger(fn LogFunc) {
	if fn == nil {
		return
	}
	logMu.Lock()
	logger = fn
	logMu.Unlock()
}

func logf(format string, args ...any) {
	logMu.RLock()
	fn := logger
	logMu.RUnlock()
	fn(format, args...)
}

func init() {
	go coverageLoop()
}

func coverageLoop() {
	coverDir := os.Getenv("GOCOVERDIR")
	if coverDir == "" {
		logf("GOCOVERDIR environment variable is not set.\n")
		return
	}

	if err := os.MkdirAll(coverDir, 0755); err != nil {
		logf("Failed to create GOCOVERDIR directory: %v\n", err)
		return
	}

	if !waitForCoverageReady(coverDir) {
		return
	}

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	backoff := 2 * time.Minute

	for {
		select {
		case <-ticker.C:
			err := flushCounters(coverDir)
			switch {
			case err == nil:
				backoff = 2 * time.Minute
			case errors.Is(err, syscall.ENOSPC):
				logf("Disk full; pausing flush for %v.\n", backoff)
				ticker.Stop()
				backoffTimer := time.NewTimer(backoff)
				if backoff < 32*time.Minute {
					backoff *= 2
				}
				select {
				case <-backoffTimer.C:
					ticker.Reset(2 * time.Minute)
				case sig := <-sigCh:
					backoffTimer.Stop()
					flushAndTerminate(sig)
				}
			}

		case sig := <-sigCh:
			ticker.Stop()
			flushAndTerminate(sig)
		}
	}
}

// waitForCoverageReady blocks until the Go runtime has finalized the
// coverage meta-data (writing the meta-data blueprint to coverDir as a
// side effect), or until it becomes clear the binary was not built with
// -cover. It returns true if coverage data can be collected.
//
// This retry is required because the runtime emits coverage meta-data
// from the *main* package's init function, which runs after every
// imported package's init, including this one. Since coverageLoop is
// launched from init() via a goroutine, an immediate WriteMetaDir can
// race ahead of that finalization and spuriously report "not built with
// -cover" before the meta hash is ready. Retrying lets program
// initialization complete before we decide coverage is unavailable.
func waitForCoverageReady(coverDir string) bool {
	const (
		retryInterval = 50 * time.Millisecond
		maxWait       = 30 * time.Second
	)
	for waited := time.Duration(0); ; waited += retryInterval {
		err := coverage.WriteMetaDir(coverDir)
		if err == nil {
			return true
		}
		if !strings.Contains(err.Error(), "not built with -cover") {
			// A genuine I/O error (e.g. disk problem). Don't disable
			// collection over a transient failure; the periodic flush
			// loop will retry.
			logf("Failed to write coverage metadata blueprint: %v\n", err)
			return true
		}
		if waited >= maxWait {
			logf("Binary not built with -cover; coverage collection disabled.\n")
			return false
		}
		time.Sleep(retryInterval)
	}
}

// flushAndTerminate stops the process on the first signal, flushing
// coverage counters on the way out.
//
// signal.Notify suppresses the default disposition (process termination)
// of the signals it watches, so merely returning from coverageLoop would
// leave the host program running -- it never observes the signal -- and the
// process would only stop on a second Ctrl+C. Re-raising the signal does
// not reliably terminate on macOS, so we exit explicitly. os.Exit runs the
// Go runtime's coverage exit hook, which writes the final counter data to
// GOCOVERDIR; that single flush avoids the double-counting that a manual
// WriteCountersDir here followed by the exit hook would produce. We exit
// with 128+signum to mirror conventional signal-termination status codes.
func flushAndTerminate(sig os.Signal) {
	logf("Received %v, flushing coverage counters before exit.\n", sig)
	code := 1
	if s, ok := sig.(syscall.Signal); ok {
		code = 128 + int(s)
	}
	os.Exit(code)
}

func flushCounters(coverDir string) error {
	err := coverage.WriteCountersDir(coverDir)
	if err == nil {
		return nil
	}

	if errors.Is(err, fs.ErrNotExist) {
		logf("Coverage directory missing; recreating %v.\n", coverDir)
		if mkErr := os.MkdirAll(coverDir, 0755); mkErr != nil {
			logf("Failed to recreate coverage directory: %v\n", mkErr)
			return err
		}
		if retryErr := coverage.WriteCountersDir(coverDir); retryErr != nil {
			logf("Retry after directory recovery failed: %v\n", retryErr)
			return retryErr
		}
		return nil
	}

	logf("Periodic coverage counter flush failed: %v\n", err)
	return err
}
