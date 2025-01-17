/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package instance

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/ssm/proto"
)

type ByUUID []proto.Instance

func (s ByUUID) Len() int           { return len(s) }
func (s ByUUID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByUUID) Less(i, j int) bool { return s[i].UUID < s[j].UUID }

type Repo struct {
	logger      *pct.Logger
	instanceDir string
	api         pct.APIConnector
	// --
	instances map[string]proto.Instance
	mux       *sync.Mutex
}

// Creates a new instance repository and returns a pointer to it
func NewRepo(logger *pct.Logger, instanceDir string, api pct.APIConnector) *Repo {
	m := &Repo{
		logger:      logger,
		instanceDir: instanceDir,
		api:         api,
		// --
		instances: make(map[string]proto.Instance),
		mux:       &sync.Mutex{},
	}
	return m
}

// Initializes the instance repository by reading system tree from local file
func (r *Repo) Init() (err error) {
	r.logger.Debug("Init:call")
	defer r.logger.Debug("Init:return")

	files, err := filepath.Glob(filepath.Join(r.instanceDir, "*"+pct.INSTANCE_FILE_SUFFIX))
	if err != nil {
		return err
	}
	r.logger.Debug(len(files), "instance files")
	for _, file := range files {
		r.logger.Debug("reading " + file)
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return fmt.Errorf("Cannot read instance file: %s: %s", file, err)
		}
		var in proto.Instance
		if err := json.Unmarshal(data, &in); err != nil {
			return fmt.Errorf("Invalid instance file: %s: %s", file, err)
		}
		// Use low-level add() because we have locked the mutex.
		if err := r.add(in, false); err != nil {
			return fmt.Errorf("Failed to add instance file: %s: %s", file, err)
		}
		r.logger.Info(fmt.Sprintf("Loaded %s %s from %s", in.Subsystem, in.Name, file))
	}

	return nil
}

func (r *Repo) List(subsystemName string) []proto.Instance {
	r.mux.Lock()
	defer r.mux.Unlock()
	instances := []proto.Instance{}
	for _, in := range r.instances {
		if in.Subsystem == subsystemName {
			instances = append(instances, in)
		}
	}
	sort.Sort(ByUUID(instances))
	return instances
}

func (r *Repo) Add(in proto.Instance, writeToDisk bool) error {
	r.logger.Debug("Add:call")
	defer r.logger.Debug("Add:return")
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.add(in, writeToDisk)
}

func (r *Repo) add(in proto.Instance, writeToDisk bool) error {
	r.logger.Debug("add:call")
	defer r.logger.Debug("add:return")

	if _, ok := r.instances[in.UUID]; ok {
		return ErrDuplicateInstance
	}

	if writeToDisk {
		if err := pct.Basedir.WriteInstance(in.UUID, in); err != nil {
			return err
		}
		r.logger.Info("Added " + in.Subsystem + " " + in.UUID)
	}

	r.instances[in.UUID] = in

	return nil
}

func (r *Repo) Get(uuid string, cache bool) (proto.Instance, error) {
	r.logger.Debug("Get:call")
	defer r.logger.Debug("Get:return")
	r.mux.Lock()
	defer r.mux.Unlock()

	// Get instance from cache (BASEDIR/instance/UUID.json).
	in, ok := r.instances[uuid]
	if ok {
		return in, nil
	}

	// Get instance from json file.
	file := filepath.Join(r.instanceDir, uuid+pct.INSTANCE_FILE_SUFFIX)
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return in, ErrInstanceNotFound(fmt.Sprintf("Cannot read instance file: %s: %s", file, err))
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return in, ErrInstanceNotFound(fmt.Sprintf("Invalid instance file: %s: %s", file, err))
	}

	// Use low-level add() because we've already locked the mutex.
	if err := r.add(in, cache); err != nil {
		return in, fmt.Errorf("Failed to add new instance %s: %s", uuid, err)
	}

	return in, nil
}

// Update updates an existing instance in the repo and optionally writes its config
// to into a json config file
func (r *Repo) Update(in proto.Instance, writeToDisk bool) error {
	r.logger.Debug("Add:call")
	defer r.logger.Debug("Add:return")
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.update(in, writeToDisk)
}

func (r *Repo) update(in proto.Instance, writeToDisk bool) error {
	r.logger.Debug("add:call")
	defer r.logger.Debug("add:return")

	if _, ok := r.instances[in.UUID]; !ok {
		return ErrDuplicateInstance
	}

	if writeToDisk {
		if err := pct.Basedir.WriteInstance(in.UUID, in); err != nil {
			return err
		}
		r.logger.Info("Added " + in.Subsystem + " " + in.UUID)
	}

	r.instances[in.UUID] = in

	return nil
}

func (r *Repo) Remove(uuid string) error {
	r.logger.Debug("Remove:call")
	defer r.logger.Debug("Remove:return")

	r.mux.Lock()
	defer r.mux.Unlock()

	file := pct.Basedir.InstanceFile(uuid)
	if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
		return err
	}
	r.logger.Info("Removed", file)

	delete(r.instances, uuid)
	r.logger.Info("Removed " + uuid)
	return nil
}

func (r *Repo) SoftRemove(uuid string) error {
	in := proto.Instance{}
	if err := pct.Basedir.ReadInstance(uuid, &in); err != nil {
		return err
	}

	delete(r.instances, uuid)
	in.Deleted = time.Now()
	return pct.Basedir.WriteInstance(uuid, in)
}
