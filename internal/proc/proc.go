package proc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Proc struct {
	FileName string
	Args     []string
	Env      map[string]string

	BinLog func(msgs ...[]byte)
	RunLog func(format string, args ...any)

	cmd *exec.Cmd

	logs chan []byte

	wg sync.WaitGroup
	rg sync.WaitGroup

	once sync.Once
	done chan struct{}
}

func (p *Proc) Start() error {
	if _, err := os.Stat(p.FileName); err != nil {
		return fmt.Errorf("invalid executable path: %w", err)
	}

	if p.BinLog == nil {
		return errors.New("BinLog is nil")
	}
	if p.RunLog == nil {
		return errors.New("RunLog is nil")
	}

	reps := make([]string, 0, len(p.Env)*2)
	for k, v := range p.Env {
		if strings.HasPrefix(k, "MESS_") {
			reps = append(reps, "{"+k+"}", v)
		}
	}
	rep := strings.NewReplacer(reps...)

	args := make([]string, 0, len(p.Args))
	for _, arg := range p.Args {
		args = append(args, rep.Replace(arg))
	}

	cmd := exec.Command(p.FileName, args...)
	cmd.Dir = filepath.Dir(p.FileName)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	env := os.Environ()
	for k, v := range p.Env {
		env = append(env, k+"="+rep.Replace(v))
	}
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err = cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	p.cmd = cmd

	p.done = make(chan struct{})
	p.logs = make(chan []byte, 128)

	oomPath := filepath.Join("/proc", strconv.Itoa(p.cmd.Process.Pid), "oom_score_adj")
	if err = os.WriteFile(oomPath, []byte("0"), 0o640); err != nil {
		p.logf("error setting oom_score_adj: %v", err)
	}

	p.rg.Add(2)
	go p.readLogs(stdout)
	go p.readLogs(stderr)

	p.wg.Add(2)
	go p.writeLogs()
	go func() {
		defer p.wg.Done()
		defer close(p.done)
		defer close(p.logs)
		defer p.rg.Wait()

		if e := p.cmd.Wait(); e != nil {

			var exitErr *exec.ExitError
			if errors.As(e, &exitErr) {
				if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
					if ws.Signal() == syscall.SIGKILL {
						return
					}
				}
			}
			p.logf("process exited with error: %v", e)
		}

	}()

	return nil
}

func (p *Proc) writeLogs() {
	defer p.wg.Done()

	if p.BinLog == nil {
		for range p.logs {
		}
		return
	}

	logs := make([][]byte, 0, 32)

	tick := time.NewTicker(time.Second / 2)
	defer tick.Stop()

	for {
		select {
		case l, ok := <-p.logs:
			if !ok {
				if len(logs) > 0 {
					p.BinLog(logs...)
				}
				return
			}
			logs = append(logs, l)

			if len(logs) == cap(logs) {
				p.BinLog(logs...)
				logs = logs[:0]
			}

		case <-tick.C:
			if len(logs) > 0 {
				p.BinLog(logs...)
				logs = logs[:0]
			}
		}
	}
}

func (p *Proc) readLogs(r io.Reader) {
	defer p.rg.Done()

	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {
			if clean := bytes.TrimSpace(line); len(clean) > 0 {
				p.logs <- clean
			}
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.logf("log scan error: %v", err)
			}
			return
		}
	}
}

func (p *Proc) Done() <-chan struct{} {
	if p == nil || p.done == nil {
		panic("proc: Done called before Start")
	}
	return p.done
}

func (p *Proc) Running() bool {
	if p == nil || p.done == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *Proc) logf(format string, args ...any) {
	p.RunLog(format, args...)
}

func (p *Proc) Close(timeout int) {
	if p == nil || p.cmd == nil {
		return
	}
	p.once.Do(func() {

		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			p.logf("signal error (term): %v", err)
		}

		select {
		case <-p.done:

		case <-time.After(time.Duration(timeout) * time.Second):
			p.logf("process shutdown timeout reached, trying to kill")

			if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				p.logf("signal error (kill): %v", err)
			}

			select {
			case <-p.done:
			case <-time.After(time.Second):
				p.logf("process still alive, trying to kill a group")
				if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
					p.logf("signal error (group kill): %v", err)
				}
				<-p.done
			}
		}

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			p.logf("log writer, leaving")
		}
	})
}
