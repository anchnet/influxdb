package export

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr     io.Writer
	Stdout     io.Writer
	Logger     *zap.Logger
	MetaClient MetaClient

	cpu *os.File
	mem *os.File

	configPath      string
	cpuProfile      string
	memProfile      string
	database        string
	rp              string
	shardDuration   time.Duration
	retentionPolicy string
	startTime       int64
	endTime         int64
	format          string
	print           bool
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	config, err := cmd.ParseConfig(cmd.getConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Validate the configuration.
	if err = config.Validate(); err != nil {
		return fmt.Errorf("validate config: %s", err)
	}

	var logErr error
	if cmd.Logger, logErr = config.Logging.New(cmd.Stderr); logErr != nil {
		// assign the default logger
		cmd.Logger = logger.New(cmd.Stderr)
	}

	if err = cmd.openMetaClient(config, cmd.Logger); err != nil {
		return err
	}

	e, err := cmd.openExporter(config, cmd.Logger)
	if err != nil {
		return err
	}
	defer e.Close()

	if cmd.print {
		e.PrintPlan(os.Stdout)
		return nil
	}

	cmd.startProfile()
	defer cmd.stopProfile()

	var wr format.Writer
	switch cmd.format {
	case "line":
		wr = line.NewWriter(os.Stdout)

	case "binary":
		wr = binary.NewWriter(os.Stdout, cmd.database, cmd.rp, cmd.shardDuration)
	}
	defer func() {
		err = wr.Close()
	}()

	return e.WriteTo(wr)
}

func (cmd *Command) openMetaClient(config *run.Config, log *zap.Logger) error {
	client := meta.NewClient(config.Meta)
	client.WithLogger(log)
	if err := client.Open(); err != nil {
		return err
	}

	cmd.MetaClient = client
	return nil
}

func (cmd *Command) openExporter(config *run.Config, log *zap.Logger) (*Exporter, error) {
	cfg := &Config{Database: cmd.database, RP: cmd.rp, ShardDuration: cmd.shardDuration, Data: config.Data}
	e, err := NewExporter(cmd.MetaClient, cfg, log)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.StringVar(&cmd.format, "format", "line", "Output format (line, binary)")
	fs.BoolVar(&cmd.print, "print", false, "Print plan to stdout")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.format != "line" && cmd.format != "binary" {
		return fmt.Errorf("invalid format '%s'", cmd.format)
	}

	return nil
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*run.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		return nil, errors.New("missing config file")
	}

	config := run.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

// GetConfigPath returns the config path from the options.
// It will return a path by searching in this order:
//   1. The CLI option in ConfigPath
//   2. The environment variable INFLUXDB_CONFIG_PATH
//   3. The first influxdb.conf file on the path:
//        - ~/.influxdb
//        - /etc/influxdb
func (cmd *Command) getConfigPath() string {
	if cmd.configPath != "" {
		if cmd.configPath == os.DevNull {
			return ""
		}
		return cmd.configPath
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

// StartProfile initializes the cpu and memory profile, if specified.
func (cmd *Command) startProfile() {
	if cmd.cpuProfile != "" {
		f, err := os.Create(cmd.cpuProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.cpu = f
		pprof.StartCPUProfile(cmd.cpu)
	}

	if cmd.memProfile != "" {
		f, err := os.Create(cmd.memProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "memprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func (cmd *Command) stopProfile() {
	if cmd.cpu != nil {
		pprof.StopCPUProfile()
		cmd.cpu.Close()
	}
	if cmd.mem != nil {
		pprof.Lookup("heap").WriteTo(cmd.mem, 0)
		cmd.mem.Close()
	}
}
