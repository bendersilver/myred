package myred

import (
	"bufio"
	"os/exec"

	"golang.org/x/sync/errgroup"
)

func (s *Stream) cmdRun(bin string, args []string, errWrapper, fnWrapper func(string) error) error {
	bin, err := exec.LookPath(bin)
	if err != nil {
		return err
	}
	s.cmd = exec.Command(bin, args...)
	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return err
	}
	defer stderr.Close()

	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	s.cmd.Start()

	g := new(errgroup.Group)
	g.Go(func() (err error) {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			err = errWrapper(scanner.Text())
			if err != nil {
				break
			}
		}
		return
	})
	g.Go(func() error {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			err = fnWrapper(scanner.Text())
			if err != nil {
				break
			}
		}
		return nil
	})
	g.Go(s.cmd.Wait)
	return g.Wait()
}
