package cmd

import (
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
)

var logger = utils.GetLogger("juicefs")
var cliCtx *cli.Context

func Main(args []string) error {
	app := &cli.App{
		Name:      "juicefs",
		Usage:     "A POSIX file system built on object storage",
		Version:   "1.3",
		Copyright: "Apache 2.0",
		Commands: []*cli.Command{
			cmdSync(),
			cmdDashboard(),
		},
		Flags: globalFlags(),
	}

	return app.Run(args)
}

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "verbose",
			Usage: "enable verbose output",
		},
		&cli.BoolFlag{
			Name:  "quiet",
			Usage: "only warning and errors",
		},
		&cli.StringFlag{
			Name:  "trace",
			Usage: "enable tracing",
		},
		&cli.StringFlag{
			Name:  "log-level",
			Usage: "set log level",
		},
	}
}

func setup0(c *cli.Context, min, max int) {
	if c.NArg() < min {
		logger.Fatalf("This command requires at least %d arguments\n", min)
	} else if max > 0 && c.NArg() > max {
		logger.Fatalf("This command accept at most %d arguments but got %+v\n", max, c.Args().Slice())
	}
}

func setup(c *cli.Context, n int) {
	setup0(c, n, n)
}

func exposeMetrics(c *cli.Context, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	return "127.0.0.1:9567"
}

func removePassword(srcURL, dstURL string) {}

func expandFlags(flags ...[]cli.Flag) []cli.Flag {
	var result []cli.Flag
	for _, f := range flags {
		result = append(result, f...)
	}
	return result
}

func addCategories(cat string, flags []cli.Flag) []cli.Flag {
	return flags
}
