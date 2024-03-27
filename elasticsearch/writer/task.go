package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/as-tool/as-etl-engine/common/element"

	"github.com/as-tool/as-etl-engine/core/plugin"
	"github.com/as-tool/as-etl-engine/core/spi/writer"

	"github.com/olivere/elastic/v7"
)

// Task
type Task struct {
	*writer.BaseTask

	IndexName              string
	TypeName               string
	BatchSize              int64
	EnableRedundantColumn  bool
	ColumnList             []EsColumn
	CombinedIdColumn       *EsColumn
	Splitter               string
	TypeList               []string
	PrimaryKeyInfo         *PrimaryKeyInfo
	ColNameToIndexMap      map[string]int
	EsPartitionColumn      []PartitionColumn
	DeleteByConditions     []map[string]interface{}
	Client                 *elastic.Client
	ActionType             string
	EnableWriteNull        bool
	IsGreaterOrEqualThan7  bool
	trySize                int64
	tryInterval            int64
	RetryTimes             int64
	SleepTimeInMilliSecond int64
	UrlParams              map[string]interface{}
	FieldDelimiter         string

	hasPrimaryKeyInfo    bool
	hasEsPartitionColumn bool
	columnSizeChecked    bool
}

func (t *Task) Init(ctx context.Context) (err error) {
	// 一开始就初始化的，没有配置的
	t.hasPrimaryKeyInfo = false
	t.hasEsPartitionColumn = false
	t.columnSizeChecked = false
	// 能配置的
	conf := t.PluginJobConf()
	t.IndexName = GetIndexName(conf)
	t.trySize = GetTrySize(conf)
	t.tryInterval = GetTryInterval(conf)
	t.BatchSize = GetBatchSize(conf)
	t.Splitter = GetSplitter(conf)
	t.ActionType = GetActionType(conf).String()
	t.UrlParams = GetUrlParams(conf)
	t.EnableWriteNull = IsEnableNullUpdate(conf)
	t.RetryTimes = GetRetryTimes(conf)
	t.SleepTimeInMilliSecond = GetSleepTimeInMilliSecond(conf)
	t.IsGreaterOrEqualThan7 = IsGreaterOrEqualThan7(conf, t.Client)
	t.DeleteByConditions = ParseDeleteCondition(conf)
	t.ColumnList = GetWriteColumns(conf)
	hasId := IsHasId(conf)
	if hasId {
		slog.Info("Task has id column, will use it to set _id property")
	} else {
		slog.Info("Task will use elasticsearch auto generated _id property")
	}
	t.FieldDelimiter = GetFieldDelimiter(conf)
	t.EnableRedundantColumn = getEnableRedundantColumn(conf)

	var typeList = make([]string, 0)
	t.CombinedIdColumn = GetcombinedIdColumn(t.ColumnList, &typeList)
	t.TypeList = typeList

	t.PrimaryKeyInfo = GetPrimaryKeyInfo(conf)
	t.EsPartitionColumn = GetEsPartitionColumn(conf)
	t.ColNameToIndexMap = make(map[string]int)

	t.Client = ES_init(conf)
	return

}

func (w *Task) Destroy(ctx context.Context) error {
	return nil
}

func (t *Task) StartWrite(ctx context.Context, receiver plugin.RecordReceiver) (err error) {
	var writerBuffer []element.Record = make([]element.Record, 0)
	for {
		var record element.Record
		record, err = receiver.GetFromReader()
		if err != nil {
			message := fmt.Sprintf("no data,%v", err)
			slog.Error(message)
			break
		}
		if !t.columnSizeChecked {
			isInvalid := true
			if t.EnableRedundantColumn {
				// 允许重复列
				isInvalid = len(t.ColumnList) > record.ColumnNumber()
			} else {
				isInvalid = len(t.ColumnList) != record.ColumnNumber()
			}
			if isInvalid {
				message := fmt.Sprintf("column number not equal error, reader column size is %d, but the writer column size is %d", record.ColumnNumber(), len(t.ColumnList))
				return errors.New(message)
			}
			// 就检查本次列数
			t.columnSizeChecked = true
		}
		writerBuffer = append(writerBuffer, record)
		if len(writerBuffer) >= int(t.BatchSize) {
			t.DoBatchInsert(writerBuffer)
			writerBuffer = make([]element.Record, 0)
		}
	}
	if len(writerBuffer) > 0 {
		t.DoBatchInsert(writerBuffer)
	}
	return nil
}

func (t *Task) DoBatchInsert(writerBuffer []element.Record) error {
	bulkRequest := t.Client.Bulk()
	ctx := context.Background()
	totalNumber := len(writerBuffer)
	dirtyDataNumber := 0
	// TODO urlParam
	for _, record := range writerBuffer {
		data := make(map[string]interface{})
		var id, parent, routing, version, columnName string
		var column element.Column
		var err error
		for i := 0; i < record.ColumnNumber(); i++ {
			column, err = record.GetByIndex(i)
			if err != nil {
				continue
			}
			columnName = t.ColumnList[i].Name
			if t.CombinedIdColumn != nil {
				if contains(t.CombinedIdColumn.CombineFields, columnName) {
					// 如果组合id 不为空，需要把相关字段全忽略
					continue
				}
			}
			var columnType string
			if t.ColumnList[i].JsonArray {
				columnType = NESTED.String()
			} else {
				columnType = t.TypeList[i]
			}
			// 如果是json数组
			dstArray := t.ColumnList[i].DstArray
			columnStr, err := column.AsString()
			if t.ColumnList[i].Array && err == nil {
				dataList := strings.Split(columnStr, t.Splitter)
				if columnType != DATE.String() {
					if dstArray {
						switch columnType {
						case BYTE.String(), KEYWORD.String(), TEXT.String():
							data[columnName] = dataList
						case SHORT.String(), INTEGER.String():
							if strings.TrimSpace(columnStr) == "" {
								data[columnName] = nil
							} else {
								var intDataList []int = make([]int, 0)
								for j := 0; j < len(dataList); j++ {
									if strings.TrimSpace(dataList[j]) != "" {
										v, _ := strconv.Atoi(dataList[j])
										intDataList = append(intDataList, v)
									}
								}
								data[columnName] = intDataList
							}
						case FLOAT.String():
							if columnStr == "" {
								data[columnName] = nil
							} else {
								var intDataList []float64 = make([]float64, 0)
								for j := 0; j < len(dataList); j++ {
									if strings.TrimSpace(dataList[j]) != "" {
										v, _ := strconv.ParseFloat(dataList[j], 64)
										intDataList = append(intDataList, v)
									}
								}
								data[columnName] = intDataList
							}
						default:
							data[columnName] = dataList
						}
					} else {
						data[columnName] = dataList
					}
				} else {
					data[columnName] = dataList
				}
			} else {
				// 不是数组类型
				switch columnType {
				case ID.String():
					if id != "" {
						id += columnStr
					} else {
						id = columnStr
					}

				case PARENT.String():
					if parent != "" {
						parent += columnStr
					} else {
						parent = columnStr
					}

				case ROUTING.String():
					if routing != "" {
						routing += columnStr
					} else {
						routing = columnStr
					}

				case VERSION.String():
					if version != "" {
						version += columnStr
					} else {
						version = columnStr
					}
				case DATE.String():
					dateStr := getDateStr(t.ColumnList[i], column)
					data[columnName] = dateStr
				case KEYWORD.String(), STRING.String(), TEXT.String(), IP.String(), GEO_POINT.String(), IP_RANGE.String():
					data[columnName] = columnStr
				case BOOLEAN.String():
					columnBol, _ := column.AsBool()
					data[columnName] = columnBol
				case BYTE.String(), BINARY.String():
					// json序列化不支持byte类型，es支持的binary类型，必须传入base64的格式
					data[columnName] = columnStr
				case LONG.String(), INTEGER.String(), SHORT.String():
					columnLong, _ := column.AsInt64()
					data[columnName] = columnLong
				case FLOAT.String(), DOUBLE.String():
					columnFloat, _ := column.AsFloat64()
					data[columnName] = columnFloat
				case GEO_SHAPE.String(), DATE_RANGE.String(), INTEGER_RANGE.String(), FLOAT_RANGE.String(), LONG_RANGE.String(), DOUBLE_RANGE.String():
					if columnStr == "" {
						data[columnName] = ""
					} else {
						v, _ := json.Marshal(columnStr)
						data[columnName] = v
					}
				case NESTED.String(), OBJECT.String():
					if columnStr == "" {
						data[columnName] = ""
					} else {
						v, _ := json.Marshal(columnStr)
						data[columnName] = v
					}
				default:
					message := fmt.Sprintf("Type error: unsupported type %s for column %s", columnType, columnName)
					return errors.New(message)
				}
			}
		}

		if t.hasPrimaryKeyInfo {
			var idData []string = make([]string, 0)
			for _, eachCol := range t.PrimaryKeyInfo.Column {
				con := t.ColNameToIndexMap[eachCol]
				recordColumn, err := record.GetByIndex(con)
				if err != nil {
					return err
				}
				idData = append(idData, recordColumn.String())
			}
			id = strings.Join(idData, t.PrimaryKeyInfo.FieldDelimiter)
		}

		if t.hasEsPartitionColumn {
			var idData []string = make([]string, 0)
			for _, eachCol := range t.EsPartitionColumn {
				con := t.ColNameToIndexMap[eachCol.Name]
				recordColumn, err := record.GetByIndex(con)
				if err != nil {
					continue
				}
				idData = append(idData, recordColumn.String())
			}
			routing = strings.Join(idData, "")
		}

		if t.IsDeleteRecord(record) {
			doc := elastic.NewBulkDeleteRequest().Id(id)
			bulkRequest = bulkRequest.Add(doc)
		} else {

			if t.CombinedIdColumn != nil {
				_, err := t.processIDCombineFields(record, t.CombinedIdColumn)
				if err != nil {
					dirtyDataNumber++
					continue
				}
			}

			switch t.ActionType {

			case INDEX.String():
				var doc = &elastic.BulkIndexRequest{}
				// 固定写法 参看elastic.NewBulkIndexRequest()
				doc.OpType("index")
				doc.Index(t.IndexName)
				if !t.IsGreaterOrEqualThan7 {
					doc.Type(t.TypeName)
				}
				if t.EnableWriteNull {
					dt, err := json.Marshal(data)
					if err != nil {
						// TODO go允许写null的写法
					} else {
						doc.Doc(string(dt))
					}
				} else {
					dt, err := json.Marshal(data)
					if err != nil {
						// TODO go允许写null的写法
					} else {
						doc.Doc(dt)
					}
				}
				if id != "" {
					doc.Id(id)
				}
				if parent != "" {
					doc.Parent(parent)
				}
				if routing != "" {
					doc.Routing(routing)
				}
				if version != "" {
					v, _ := strconv.ParseInt(version, 10, 64)
					doc.Version(v)
					doc.VersionType("external")
				}

				bulkRequest = bulkRequest.Add(doc)

			case UPDATE.String():
				updateDoc := &elastic.BulkUpdateRequest{}
				updateDoc.Index(t.IndexName)
				if !t.IsGreaterOrEqualThan7 {
					updateDoc.Type(t.TypeName)
				}
				updateArr := make(map[string]interface{})
				updateArr["doc"] = data
				updateArr["doc_as_upsert"] = true
				if t.EnableWriteNull {
					dt, _ := json.Marshal(updateArr)
					updateDoc.Doc(dt)
				} else {
					dt, _ := json.Marshal(updateArr)
					updateDoc.Doc(dt)
				}
				if id != "" {
					updateDoc.Id(id)
				}
				if parent != "" {
					updateDoc.Parent(parent)
				}
				if routing != "" {
					updateDoc.Routing(routing)
				}
				if version != "" {
					v, _ := strconv.ParseInt(version, 10, 64)
					updateDoc.Version(v)
				}
				bulkRequest = bulkRequest.Add(updateDoc)
			}
		}
	}
	if dirtyDataNumber >= totalNumber {
		message := fmt.Sprintf("all this batch is dirty data, dirtyDataNumber: %d totalDataNumber: %d", dirtyDataNumber,
			totalNumber)
		return errors.New(message)
	}
	ExecuteWithRetry(doOperate, bulkRequest, ctx, int(t.trySize), time.Duration(t.tryInterval))

	return nil
}

// 重试函数，接受一个操作函数、最大重试次数以及重试间隔
func ExecuteWithRetry(operation func(*elastic.BulkService, context.Context) (bool, error), bulkRequest *elastic.BulkService, ctx context.Context, maxRetries int, retryInterval time.Duration) (success bool, err error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		success, err = operation(bulkRequest, ctx)
		if err == nil {
			// 操作成功，跳出循环
			break
		}
		if success {
			// 操作失败，重试不了了
			slog.Error(fmt.Sprintf("Operation failed,%s ", err))
			break
		}
		// 操作失败，等待一段时间后重试
		slog.Error(fmt.Sprintf("Operation failed, retrying after %v (attempt %d/%d)\n", retryInterval.String(), attempt+1, maxRetries))

		time.Sleep(retryInterval)
	}
	return success, err
}

func doOperate(bulkRequest *elastic.BulkService, ctx context.Context) (bool, error) {
	response, err := bulkRequest.Do(ctx)
	if err != nil {
		return false, err
	}
	failed := response.Failed()
	l := len(failed)
	if l > 0 {
		msg := fmt.Sprintf("Error(%d)%t", l, response.Errors)
		// 返回true 因为数据为空了，执行不了下一次重试了
		return true, errors.New(msg)
	}
	return true, nil
}

func contains(arr []string, a string) bool {
	for _, value := range arr {
		return value == a
	}
	return false
}

func getDateStr(esColumn EsColumn, column element.Column) string {
	if esColumn.Origin {
		v, _ := column.AsString()
		return v
	}
	var dtz *time.Location
	dtz, _ = time.LoadLocation("")
	if esColumn.Timezone != "" {
		dtz, _ = time.LoadLocation(esColumn.Timezone)
	}

	if column.Type() != element.TypeTime && esColumn.Format != "" {
		v, _ := column.AsString()
		date, _ := time.Parse("2006/01/02 15:04:05", v)
		date = date.In(dtz)
		return date.Format(esColumn.Format)
	} else if column.Type() == element.TypeTime {
		if column.IsNil() {
			return ""
		} else {
			v, _ := column.AsTime()
			// 格式化时间，转换为本地时间
			localTime := v.In(dtz)
			// 转换为字符串
			formattedTime := localTime.Format("2006-01-02T15:04:05Z")
			return formattedTime
		}
	} else {
		v, _ := column.AsString()
		return v
	}
}

func (t *Task) IsDeleteRecord(record element.Record) bool {
	if t.DeleteByConditions == nil {
		return false
	}
	kv := make(map[string]interface{})
	for i := 0; i < record.ColumnNumber(); i++ {
		column, err := record.GetByIndex(i)
		if err != nil {
			continue
		}
		columnName := t.ColumnList[i].Name
		columnStr, err := column.AsString()
		if err != nil {
			continue
		}
		kv[columnName] = columnStr
	}
	for _, v := range t.DeleteByConditions {
		if !meetAllCondition(kv, v) {
			return true
		}
	}
	return false
}

func meetAllCondition(kv, delCondition map[string]interface{}) bool {
	for k, v := range delCondition {
		if !checkOneCondition(kv, k, v) {
			return false
		}
	}
	return true
}

func checkOneCondition(kv map[string]interface{}, entrykey string, entryValue interface{}) bool {
	value := kv[entrykey]
	t := reflect.ValueOf(entryValue)
	if t.Kind() == reflect.Slice {
		var op = make([]interface{}, 0)
		op = value.([]interface{})
		for _, v := range op {
			if v == value {
				return true
			}
		}
	} else {
		if value != nil && value == entryValue {
			return true
		}
	}
	return false
}

func (t *Task) processIDCombineFields(record element.Record, esColumn *EsColumn) (string, error) {
	values := make([]string, 0)
	for _, field := range esColumn.CombineFields {
		colIndex, _ := t.getRecordColumnIndex(record, field)
		var col element.Column
		if record.ColumnNumber() <= colIndex {
			col = nil
		} else {
			c, err := record.GetByIndex(colIndex)
			if err != nil {
				col = nil
			} else {
				col = c
			}
		}
		if col == nil {
			message := fmt.Sprintf("%s RECORD FIELD NOT FOUND", field)
			return "", errors.New(message)
		}
		colStr, _ := col.AsString()
		values = append(values, colStr)
	}
	return strings.Join(values, esColumn.CombineFieldsValueSeparator), nil
}

func (t *Task) getRecordColumnIndex(record element.Record, columnName string) (int, error) {
	idx, ok := t.ColNameToIndexMap[columnName]
	if ok {
		return idx, nil
	}

	var columns []element.Column

	index := -1
	for i := 0; i < record.ColumnNumber(); i++ {
		column, err := record.GetByIndex(i)
		if err != nil {
			continue
		}
		colName := t.ColumnList[i].Name
		if colName == columnName {
			columns = append(columns, column)
			index = i
		}
	}

	if len(columns) <= 0 {
		message := fmt.Sprintf("%s RECORD FIELD NOT FOUND", columnName)
		return 0, errors.New(message)
	}

	if len(columns) > 1 {
		message := fmt.Sprintf("record has multiple columns found by name: %s", columnName)
		return 0, errors.New(message)
	}

	t.ColNameToIndexMap[columnName] = index
	return index, nil
}
