package elasticsearch

import (
	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/core/plugin"

	spiwriter "github.com/as-tool/as-etl-engine/core/spi/writer"
)

// Writer Writer
type Writer struct {
	pluginConf *config.JSON
}

// ResourcesConfig Plugin Resource Configuration
func (w *Writer) ResourcesConfig() *config.JSON {
	return w.pluginConf
}

// Job Job
func (w *Writer) Job() spiwriter.Job {
	job := &Job{
		BaseJob: plugin.NewBaseJob(),
	}
	job.SetPluginConf(w.pluginConf)
	return job
}

// Task Task
func (w *Writer) Task() spiwriter.Task {
	task := &Task{
		BaseTask: spiwriter.NewBaseTask(),
	}
	task.SetPluginConf(w.pluginConf)
	return task
}
