// Copyright 2020 the go-etl Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"github.com/as-tool/as-etl-engine/common/config"
	spireader "github.com/as-tool/as-etl-engine/core/spi/reader"
	"github.com/as-tool/as-etl-storage/database"
	dbms "github.com/as-tool/as-etl-storage/database/dbms/reader"

	// mysql storage - MySQL database storage
	_ "github.com/as-tool/as-etl-storage/database/mysql"
)

// Reader - A component or tool used for reading data from a source
type Reader struct {
	pluginConf *config.JSON
}

// ResourcesConfig - Configuration for the resources used by a plugin
func (r *Reader) ResourcesConfig() *config.JSON {
	return r.pluginConf
}

// Job - A unit of work or task to be performed
func (r *Reader) Job() spireader.Job {
	job := &Job{
		Job: dbms.NewJob(dbms.NewBaseDbHandler(func(name string, conf *config.JSON) (q dbms.Querier, err error) {
			if q, err = database.Open(name, conf); err != nil {
				return nil, err
			}
			return
		}, nil)),
	}
	job.SetPluginConf(r.pluginConf)
	return job
}

// Task - A specific piece of work or operation within a larger context, often part of a Job
func (r *Reader) Task() spireader.Task {
	task := &Task{
		Task: dbms.NewTask(dbms.NewBaseDbHandler(func(name string, conf *config.JSON) (q dbms.Querier, err error) {
			if q, err = database.Open(name, conf); err != nil {
				return nil, err
			}
			return
		}, nil)),
	}
	task.SetPluginConf(r.pluginConf)
	return task
}
