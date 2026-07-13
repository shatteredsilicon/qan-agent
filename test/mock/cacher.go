package mock

import "fmt"

type Cacher struct {
	FilesOut []string          // test provides
	DataOut  map[string][]byte // test provides
	DataIn   []interface{}
	dataChan chan interface{}
}

func NewCacher(dataChan chan interface{}) *Cacher {
	c := &Cacher{
		dataChan: dataChan,
		DataIn:   []interface{}{},
	}
	return c
}

func (*Cacher) Start() error {
	return nil
}

func (*Cacher) Stop() error {
	return nil
}

func (c *Cacher) Write(service string, data interface{}) error {
	if c.dataChan != nil {
		c.dataChan <- data
	} else {
		c.DataIn = append(c.DataIn, data)
	}
	return nil
}

func (c *Cacher) Files(cancel <-chan struct{}) <-chan string {
	filesChan := make(chan string)
	go func() {
		for _, file := range c.FilesOut {
			filesChan <- file
		}
		close(filesChan)
	}()
	return filesChan
}

func (c *Cacher) Read(file string) ([]byte, error) {
	return c.DataOut[file], nil
}

func (c *Cacher) Remove(file string) error {
	delete(c.DataOut, file)
	return nil
}

func (c *Cacher) Has(key string) bool {
	_, ok := c.DataOut[key]
	return ok
}

func (c *Cacher) CacheKey(prefix, name string) string {
	return fmt.Sprintf("%s.%s", prefix, name)
}
