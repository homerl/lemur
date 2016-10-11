package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
	"strconv"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
	"github.com/intel-hpdd/logging/debug"
)

type (
	archiveConfig struct {
		Name   string `hcl:",key"`
		ID     int
		Region string
		Bucket string
		Prefix string
	}

	archiveSet []*archiveConfig

	aliConfig struct {
		NumThreads         int        `hcl:"num_threads"`
		Region             string     `hcl:"region"`
		Endpoint           string     `hcl:"endpoint"`
		AliAccessKeyID     string     `hcl:"ali_access_key_id"`
		AliAccessKeySecret string     `hcl:"ali_access_key_secret"`
		AliEndpoint           string     `hcl:"ali_endpoint"`
		Archives           archiveSet `hcl:"archive"`
		Myproxy     string     `hcl:"myproxy"`
		Partsize     string     `hcl:"partsize"`
		Routines     string     `hcl:"routines"`
	}
)

// Should this be configurable?
const updateInterval = 10 * time.Second

var rate metrics.Meter

func (c *aliConfig) String() string {
	return dmplugin.DisplayConfig(c)
}

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s:%s/%s", a.ID, a.Region, a.Bucket, a.Prefix)
}

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Bucket == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: bucket not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))

	}

	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func (c *aliConfig) Merge(other *aliConfig) *aliConfig {
	result := new(aliConfig)

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.Region = c.Region
	if other.Region != "" {
		result.Region = other.Region
	}

	result.Endpoint = c.Endpoint
	if other.Endpoint != "" {
		result.Endpoint = other.Endpoint
	}

	result.AliAccessKeyID = c.AliAccessKeyID
	if other.AliAccessKeyID != "" {
		result.AliAccessKeyID = other.AliAccessKeyID
	}

	
	result.AliAccessKeySecret = c.AliAccessKeySecret
	if other.AliAccessKeySecret != "" {
		result.AliAccessKeySecret = other.AliAccessKeySecret
	}

	result.Myproxy = c.Myproxy
	if other.Myproxy != "" {
		result.Myproxy = other.Myproxy
	}

	result.AliEndpoint = c.AliEndpoint
	if other.AliEndpoint != "" {
		result.AliEndpoint = other.AliEndpoint
	}

	result.Partsize = c.Partsize
	if other.Partsize != "" {
		result.Partsize = other.Partsize
	}

	result.Routines = c.Routines
	if other.Routines != "" {
		result.Routines = other.Routines
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	return result
}

func init() {
	rate = metrics.NewMeter()

	// if debug.Enabled() {
	go func() {
		var lastCount int64
		for {
			if lastCount != rate.Count() {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				lastCount = rate.Count()
			}
			time.Sleep(10 * time.Second)
		}
	}()
	// }
}

func getMergedConfig(plugin *dmplugin.Plugin) (*aliConfig, error) {
	baseCfg := &aliConfig{
		Region: "us-east-1",
	}

	var cfg aliConfig
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	return baseCfg.Merge(&cfg), nil
}


func checkaliConfiguration(cfg *aliConfig) error {
	
	debug.Printf("lcfg.AliAccessKeyID:%s,cfg.AliAccessKeySecret:%s,cfg.AliEndpoint:%s,cfg.Myproxy:%s,cfg.Partsize:%s,cfg.Routines:%s", cfg.AliAccessKeyID,cfg.AliAccessKeySecret,cfg.AliEndpoint,cfg.Myproxy,cfg.Partsize,cfg.Routines)
	var myproxy string
	myproxy = cfg.Myproxy
	var client *oss.Client
	var err error 
	if myproxy != ""{
		client, err = oss.New(cfg.AliEndpoint, cfg.AliAccessKeyID, cfg.AliAccessKeySecret,oss.Proxy(myproxy))
		if err != nil {
			return errors.Wrap(err, "No Ali credentials found; cannot initialize ali data mover")
		} else {

			if _, err := client.ListBuckets(); err != nil {
				return errors.Wrap(err, "Unable to list Ali buckets")
			} else {				
				debug.Printf("list Ali bucket is OK")
			}

		}
	} else {
		client, err = oss.New(cfg.AliEndpoint, cfg.AliAccessKeyID, cfg.AliAccessKeySecret)
		if err != nil {
			return errors.Wrap(err, "No Ali credentials found; cannot initialize ali data mover")
		} else {
			if _, err := client.ListBuckets(); err != nil {
				return errors.Wrap(err, "Unable to list Ali buckets")
			} else {				
				debug.Printf("list Ali bucket is OK")
			}
		}
	}
	
	
	return nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "failed to initialize plugin"))
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	debug.Printf("S3Mover configuration:\n%v", cfg)

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err = archive.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
	}

	if cfg.AliAccessKeyID != "" {
		os.Setenv("Ali_ACCESS_KEY_ID", cfg.AliAccessKeyID)
	}
	
	if cfg.AliAccessKeySecret != "" {
		os.Setenv("Ali_ACCESS_KEY_Secret", cfg.AliAccessKeySecret)
	}

	if err := checkaliConfiguration(cfg); err != nil {
		alert.Abort(errors.Wrap(err, "S3 config check failed"))
	}

	debug.Printf("AliMover after checkaliConfiguration:\n%v", cfg)

	// All base filesystem operations will be relative to current directory
	err = os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, a := range cfg.Archives {
		
		var client *oss.Client
		if cfg.Myproxy != ""{
			client, err = oss.New(cfg.AliEndpoint, cfg.AliAccessKeyID, cfg.AliAccessKeySecret,oss.Proxy(cfg.Myproxy))
		} else {
			client, err = oss.New(cfg.AliEndpoint, cfg.AliAccessKeyID, cfg.AliAccessKeySecret)
		}


		iPartsize, err := strconv.ParseInt(cfg.Partsize,10,64)
		if err != nil {
			iPartsize = 1
		}

		iRoutines, err := strconv.Atoi(cfg.Routines)
		if err != nil {
			iRoutines = 1
		}

		plugin.AddMover(&dmplugin.Config{
			Mover:      AliMover(client, uint32(a.ID), a.Bucket, a.Prefix,iPartsize, iRoutines),
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(a.ID),
		})
	}

	plugin.Run()
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			debug.Printf("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()
}
