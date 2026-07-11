package data

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/peterbourgon/diskv"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/ssm/proto"
	pc "github.com/shatteredsilicon/ssm/proto/config"
)

const (
	DEFAULT_CACHE_MAX_AGE   = time.Hour         // 1h
	DEFAULT_CACHE_MAX_SIZE  = 1024 * 1024 * 100 // 100 MiB
	DEFAULT_CACHE_MAX_FILES = 1000
)

type Cacher interface {
	Start() error
	Stop() error
	Write(key string, data interface{}) error
	Files(cancel <-chan struct{}) <-chan string
	Read(file string) ([]byte, error)
	Remove(file string) error
	Has(key string) bool
	CacheKey(prefix, name string) string
}

type cacheConfig struct {
	MaxAge   *time.Duration `json:",omitempty"` // seconds
	MaxSize  *uint64        `json:",omitempty"` // bytes
	MaxFiles *uint          `json:",omitempty"`
}

type cacheData struct {
	Key  string    `json:"-"`
	Ts   time.Time // when data was created (UTC)
	Data []byte    // encoded tool data
}

type cacheInfo struct {
	Ts   time.Time
	Size int
}

type DiskvCacher struct {
	dir     string
	logger  *pct.Logger
	sz      proto.Serializer
	limits  pc.DataSpoolLimits
	sigChan chan os.Signal

	dataChan               chan cacheData
	status                 *pct.Status
	cache                  *diskv.Diskv
	mux                    *sync.Mutex
	sync                   *pct.SyncChan
	continuouslyDiskErrors uint
	maxAge                 time.Duration
	maxSize                uint64
	maxFiles               uint
	fileInfos              map[string]cacheInfo
}

func NewDiskvCacher(dir string, config cacheConfig, logger *pct.Logger, sz proto.Serializer, sigChan chan os.Signal) *DiskvCacher {
	c := &DiskvCacher{
		dir:     dir,
		logger:  logger,
		sz:      sz,
		sigChan: sigChan,

		dataChan:  make(chan cacheData, WRITE_BUFFER),
		status:    pct.NewStatus([]string{"data-cacher", "data-cacher-count", "data-cacher-size"}),
		mux:       new(sync.Mutex),
		sync:      pct.NewSyncChan(),
		maxAge:    DEFAULT_CACHE_MAX_AGE,
		maxSize:   DEFAULT_CACHE_MAX_SIZE,
		maxFiles:  DEFAULT_CACHE_MAX_FILES,
		fileInfos: make(map[string]cacheInfo),
	}
	if config.MaxAge != nil {
		c.maxAge = *config.MaxAge
	}
	if config.MaxSize != nil {
		c.maxSize = *config.MaxSize
	}
	if config.MaxFiles != nil {
		c.maxFiles = *config.MaxFiles
	}

	return c
}

func (c *DiskvCacher) Start() error {
	c.status.Update("data-cacher", "Starting")

	// Create the data dir if necessary.  Normally the manager does this,
	// but it's necessary to create it here for testing.
	if err := pct.MakeDir(c.dir); err != nil {
		return err
	}

	// diskv reads all files in BasePath on startup.
	c.cache = diskv.New(diskv.Options{
		BasePath:     c.dir,
		Transform:    func(s string) []string { return []string{} },
		CacheSizeMax: CACHE_SIZE,
		Index:        &diskv.BTreeIndex{},
		IndexLess:    func(a, b string) bool { return a < b },
	})

	c.mux.Lock()
	defer c.mux.Unlock()

	for key := range c.Files(context.Background().Done()) {
		data, err := c.cache.Read(key)
		if err != nil {
			c.logger.Error("Cannot read data file", key, ":", err)
			c.cache.Erase(key)
			continue
		}

		var d cacheData
		if err = json.Unmarshal(data, &d); err != nil {
			c.logger.Error("Cannot Unmarshal data file", key, ":", err)
			c.cache.Erase(key)
			continue
		}

		c.fileInfos[key] = cacheInfo{
			Ts:   d.Ts,
			Size: len(data),
		}
	}

	go c.run()
	return nil
}

func (c *DiskvCacher) Files(cancel <-chan struct{}) <-chan string {
	return c.cache.Keys(cancel)
}

func (c *DiskvCacher) Write(key string, data interface{}) error {
	/**
	 * This method is shared: multiple goroutines call it to write data.
	 * If the data serializer (sz) is not concurrent, then we serialize
	 * access.  For example, the JSON text sz is concurrent, but the gzip
	 * sz is not because it uses internal, non-mutex-guarded buffers.
	 */
	if !c.sz.Concurrent() {
		c.mux.Lock()
		defer c.mux.Unlock()
	}

	c.logger.Debug("write:call")
	defer c.logger.Debug("write:return")

	// Serialize the data: T{} -> []byte
	encodedData, err := c.sz.ToBytes(data)
	if err != nil {
		return err
	}

	// Write data to disk.
	select {
	case c.dataChan <- cacheData{Key: key, Ts: time.Now(), Data: encodedData}:
		c.continuouslyDiskErrors = 0
	case <-time.After(1 * time.Second):
		err := fmt.Errorf("timeout caching data, length of pending data to write: %d", len(c.dataChan))

		c.continuouslyDiskErrors++
		if c.continuouslyDiskErrors > 3 {
			c.sigChan <- syscall.SIGTERM
			return err
		}

		// Let caller decide what to do.
		c.logger.Debug("write:timeout")
		return err
	}

	return nil
}

func (c *DiskvCacher) Read(file string) ([]byte, error) {
	data, err := c.cache.Read(file)
	if err != nil {
		return nil, err
	}

	var d cacheData
	if len(data) > 0 && json.Unmarshal(data, &d) == nil {
		return d.Data, nil
	}

	return data, nil
}

func (c *DiskvCacher) Stop() error {
	c.sync.Stop()
	c.sync.Wait()
	c.sz = nil
	c.cache = nil
	c.logger.Info("Stopped")
	return nil
}

func (c *DiskvCacher) Remove(file string) error {
	// Don't lock mutex yet in case this takes awhile (it shouldn't):
	if err := c.cache.Erase(file); err != nil && !os.IsNotExist(err) {
		return err
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	delete(c.fileInfos, file)
	return nil
}

func (c *DiskvCacher) Has(key string) bool {
	return c.cache.Has(key)
}

func (c *DiskvCacher) CacheKey(prefix, name string) string {
	return fmt.Sprintf("%s.%s", prefix, name)
}

func (c *DiskvCacher) run() {
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("cacher crashed: ", err)
			c.sync.Crash = true
		}
		if c.sync.IsGraceful() {
			c.logger.Info("cacher stop")
			c.status.Update("data-cacher", "Stopped")
		} else {
			c.logger.Error("cacher crash")
			c.status.Update("data-cacher", "Crashed")
		}
		c.sync.Done()
	}()

	purge := func(now time.Time) {
		n, removed := c.purge(now.UTC())
		if n == 0 {
			return
		}
		for reason, files := range removed {
			if len(files) == 0 {
				continue
			}
			switch reason {
			case "age":
				c.logger.Warn(fmt.Sprintf("Removed %d old data files", len(files)))
			case "size":
				c.logger.Warn(fmt.Sprintf("Removed %d data files to reduce spool size", len(files)))
			case "files":
				c.logger.Warn(fmt.Sprintf("Removed %d data files to reduce number of files", len(files)))
			case "purged":
				c.logger.Warn(fmt.Sprintf("Purged all %d data files", len(files)))
			default:
				c.logger.Warn(fmt.Sprintf("Removed %d data files", len(files)))
			}
		}
	}

	purgeChan := time.NewTicker(1 * time.Hour).C
	purge(time.Now())

	for {
		c.status.Update("data-spooler", "Idle")
		select {
		case data := <-c.dataChan:
			c.logger.Debug("caching " + data.Key)
			c.status.Update("data-cacher", "Caching "+data.Key)

			bytes, err := json.Marshal(data)
			if err != nil {
				c.logger.Error(err)
				continue
			}

			if err := c.cache.Write(data.Key, bytes); err != nil {
				c.logger.Error(err)
			}

			c.mux.Lock()
			c.fileInfos[data.Key] = cacheInfo{Ts: data.Ts, Size: len(data.Data)}
			c.mux.Unlock()
		case now := <-purgeChan:
			c.status.Update("data-cacher", "Purging")
			c.logger.Debug("data-cacher purging")

			purge(now)
		case <-c.sync.StopChan:
			c.status.Update("data-cacher", "Stopped")
			c.logger.Debug("data-cacher stopped")

			c.sync.Graceful()
			return
		}
	}
}

func (c *DiskvCacher) purge(now time.Time) (int, map[string][]string) {
	c.status.Update("data-cacher", "Purging")
	defer c.status.Update("data-cacher", "Idle")

	c.mux.Lock()
	defer c.mux.Unlock()

	purge := false
	if c.maxAge == 0 || c.maxSize == 0 || c.maxFiles == 0 {
		c.logger.Debug("purge:all")
		purge = true
	}

	removed := map[string][]string{
		"age":    {},
		"size":   {},
		"files":  {},
		"purged": {},
	}
	n := 0

	size := c.size()
	for file := range c.Files(context.Background().Done()) {
		if info, ok := c.fileInfos[file]; !ok || purge {
			removed["purged"] = append(removed["purged"], file)
		} else if age := now.Sub(info.Ts); age > c.maxAge {
			c.logger.Debug(fmt.Sprintf("purge:age:%d", age))
			removed["age"] = append(removed["age"], file)
		} else if size > c.maxSize {
			c.logger.Debug(fmt.Sprintf("purge:size:%d", c.size()))
			c.logger.Debug("purge:size:" + file)
			removed["size"] = append(removed["size"], file)
		} else if uint(len(c.fileInfos)) > c.maxFiles {
			c.logger.Debug(fmt.Sprintf("purge:files:%d", len(c.fileInfos)))
			removed["files"] = append(removed["files"], file)
		} else {
			continue // keep file
		}
		c.remove(file, false) // false=we've already locked mux
		n++
	}

	return n, removed
}

func (c *DiskvCacher) remove(file string, lock bool) error {
	// Don't lock mutex yet in case this takes awhile (it shouldn't):
	if err := c.cache.Erase(file); err != nil && !os.IsNotExist(err) {
		return err
	}
	if lock {
		c.mux.Lock()
		defer c.mux.Unlock()
	}
	delete(c.fileInfos, file)
	return nil
}

func (c *DiskvCacher) size() (s uint64) {
	for _, f := range c.fileInfos {
		s += uint64(f.Size)
	}
	return
}
