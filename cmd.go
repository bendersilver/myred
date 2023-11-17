package myred

import (
	"bufio"
	"context"
	"os/exec"

	"golang.org/x/sync/errgroup"
)

func cmd(bin string, args []string, errWrapper, fnWrapper func(string) error) (context.CancelFunc, error) {
	bin, err := exec.LookPath(bin)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	defer stderr.Close()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	cmd.Start()

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
	g.Go(cmd.Wait)
	return cancel, g.Wait()
}
