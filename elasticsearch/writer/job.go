package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/core/plugin"
	"github.com/olivere/elastic/v7"
)

// Job
type Job struct {
	*plugin.BaseJob
	RetryTimes             int64
	SleepTimeInMilliSecond int64
	settingsCache          *string
}

func (j *Job) Init(ctx context.Context) (err error) {
	conf := j.PluginJobConf()
	j.SleepTimeInMilliSecond = GetSleepTimeInMilliSecond(conf)
	j.RetryTimes = GetRetryTimes(conf)
	j.settingsCache = new(string)
	return
}

var mutex = &sync.Mutex{}

func (j *Job) Prepare(ctx context.Context) (err error) {
	conf := j.PluginJobConf()
	client := ES_init(conf)

	actionType := GetActionType(conf)

	hasId := HasID(conf)
	conf.Set("hasId", hasId)
	if UPDATE == (actionType) && !hasId && !HasPrimaryKeyInfo(conf) {
		message := "update mode must specify column type with id or primaryKeyInfo config"
		return errors.New(message)
	}
	setCache := j.settingsCache
	mutex.Lock()
	defer mutex.Unlock()
	jobExecuteWithRetry(doJobPrepare, client, ctx, conf, setCache, int(j.RetryTimes), time.Duration(j.SleepTimeInMilliSecond))
	return
}

func jobExecuteWithRetry(operation func(*elastic.Client, context.Context, *config.JSON, *string) (bool, error),
	client *elastic.Client, ctx context.Context, conf *config.JSON, settingsCache *string, maxRetries int, retryInterval time.Duration) (bool, error) {

	var success bool
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		success, err = operation(client, ctx, conf, settingsCache)
		if err == nil {
			// 操作成功，跳出循环
			break
		}
		// 操作失败，等待一段时间后重试
		slog.Error(fmt.Sprintf("operation failed, retrying after %s (attempt %d/%d)\n", retryInterval.String(), attempt+1, maxRetries))
		time.Sleep(retryInterval)
	}
	return success, err
}

func doJobPrepare(client *elastic.Client, ctx context.Context, conf *config.JSON, settingsCache *string) (flag bool, err error) {
	indexName := GetIndexName(conf)
	typeName := GetTypeName(conf)
	dynamic := GetDynamic(conf)
	dstDynamic := GetDstDynamic(conf)
	newSettings, _ := json.Marshal(GetSettings(conf))
	slog.Info(fmt.Sprintf("conf settings:%v, settingsCache:%v", newSettings, settingsCache))

	isGreaterOrEqualThan7 := IsGreaterOrEqualThan7(conf, client)

	mappings := GenMappings(dstDynamic, typeName, isGreaterOrEqualThan7, conf)
	slog.Info(fmt.Sprintf("index:[%s], type:[%s], mappings:[%s]", indexName, typeName, mappings))
	// conf.set("isGreaterOrEqualThan7", isGreaterOrEqualThan7)
	var isIndicesExists bool
	isIndicesExists, _ = client.IndexExists(indexName).Do(ctx)
	if isIndicesExists {
		oldMappings, _ := client.GetMapping().Index(indexName).Do(ctx)
		slog.Info(fmt.Sprintf("the mappings for old index is: %v", oldMappings))
	}
	if IsTruncate(conf) && isIndicesExists {
		// 备份老的索引中的settings到缓存
		oldOriginSettings, _ := client.IndexGetSettings().Index(indexName).Do(ctx)
		if oldOriginSettings != nil {
			setts := oldOriginSettings[indexName]
			for _, v := range setts.Settings {
				vJson, _ := json.Marshal(v)
				flagJson := convertSettings(string(vJson))
				slog.Info(fmt.Sprintf("merge1 settings:%v, settingsCache:%v,", flagJson, settingsCache))

				setSettings(settingsCache, flagJson)
			}
		}
		_, err = client.DeleteIndex(indexName).Do(ctx)
		if err != nil {
			panic(err)
		}
	}

	// 更新缓存中的settings
	setSettings(settingsCache, string(newSettings))
	// 再次查询，上面有可能删除了
	isIndicesExists, _ = client.IndexExists(indexName).Do(ctx)
	if !isIndicesExists {

		body := GenBody(*settingsCache, mappings, dynamic)
		fmt.Println(body)
		createIndex, err := client.CreateIndex(indexName).BodyString(body).Do(ctx)

		if err != nil {
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged ,创建失败
			fmt.Println("Not acknowledged")
		}
	}
	return true, nil
}

func convertSettings(dataStr string) string {
	var obj map[string]interface{}
	// 解码JSON1到obj1
	json.Unmarshal([]byte(dataStr), &obj)

	keys := []string{"number_of_shards", "number_of_replicas"}
	newObj := make(map[string]interface{}, 0)
	for _, v2 := range keys {
		newObj[v2] = obj[v2]
	}
	json, _ := json.Marshal(newObj)
	return string(json)
}

func setSettings(json1 *string, json2 string) {

	if *json1 == "" || *json1 == "{}" {
		*json1 = json2
		return
	}

	if json2 == "" || json2 == "{}" {
		return
	}

	var obj1 map[string]interface{}
	var obj2 map[string]interface{}

	// 解码JSON1到obj1
	json.Unmarshal([]byte(*json1), &obj1)

	// 解码JSON2到obj2
	json.Unmarshal([]byte(json2), &obj2)

	// 合并两个对象,按下列keys条件合并
	for k, v := range obj2 {
		obj1[k] = v
	}

	// 将合并后的对象编码回JSON
	mergedJSON, _ := json.Marshal(&obj1)

	*json1 = string(mergedJSON)
}

func (j *Job) Post(ctx context.Context) (err error) {
	return
}

func (j *Job) Destroy(ctx context.Context) (err error) {
	return
}

func (j *Job) Split(ctx context.Context, number int) (confs []*config.JSON, err error) {
	for i := 0; i < number; i++ {
		confs = append(confs, j.PluginJobConf().CloneConfig())
	}
	return confs, nil
}

type Body struct {
	Settings interface{} `json:"settings"`
	Mappings interface{} `json:"mappings"`
}

func GenBody(settings, mappings string, dynamic bool) string {
	if settings == "" || settings == "{}" {
		settings = `{
			"number_of_shards": 1,
			"number_of_replicas": 0
		}`
	}
	var settingsMap map[string]interface{}
	// 解析JSON字符串到map中
	json.Unmarshal([]byte(settings), &settingsMap)
	body := &Body{
		Settings: settingsMap,
	}
	if !dynamic {
		var mappingsMap map[string]interface{}
		// 解析JSON字符串到map中
		json.Unmarshal([]byte(mappings), &mappingsMap)

		body.Mappings = mappingsMap
	}

	jsonData, _ := json.Marshal(body)
	return string(jsonData)
}

func GenMappings(dstDynamic, typeName string, isGreaterOrEqualThan7 bool, conf *config.JSON) string {
	var mappings string
	propMap := make(map[string]interface{})

	columnList := make([]EsColumn, 0)
	var combineItem EsColumn

	arr, err := conf.GetArray("column")
	if err != nil {
		return ""
	}

	if len(arr) > 0 {
		for _, col := range arr {
			colName, _ := col.GetString("name")
			colTypeStr, _ := col.GetString("type")
			if colTypeStr == "" {
				message := fmt.Sprintf("%v column must have type", col)
				log.Fatal(message)
				return ""
			}
			colType := GetESFieldType(colTypeStr)
			if colType == -1 {
				// throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " unsupported type");
				message := fmt.Sprintf("%v unsupported type", col)
				log.Fatal(message)
				return ""
			}

			var columnItem = &EsColumn{}

			if colName == "pk" {
				// 兼容已有版本
				colType = ID
				colTypeStr = "id"
			}

			columnItem.Name = colName
			columnItem.Type = colTypeStr

			combineFields, _ := col.GetArray("combineFields")
			if len(combineFields) > 0 && ID == GetESFieldType(colTypeStr) {
				fields := make([]string, 0)

				for _, item := range combineFields {
					fields = append(fields, item.String())
				}
				columnItem.CombineFields = fields
				combineItem = *columnItem
			}

			combineFieldsValueSeparator, _ := col.GetString("combineFieldsValueSeparator")
			if combineFieldsValueSeparator != "" {
				columnItem.CombineFieldsValueSeparator = combineFieldsValueSeparator
			}

			// 如果是id，version，routing，不需要创建mapping
			if colType == ID || colType == VERSION || colType == ROUTING {
				columnList = append(columnList, *columnItem)
				continue
			}

			// 如果是组合id中的字段，不需要创建mapping
			// 所以组合id的定义必须要在columns最前面
			if contains(combineItem.CombineFields, colName) {
				columnList = append(columnList, *columnItem)
				continue
			}
			columnItem.DstArray = false
			array, _ := col.GetBool("array")
			columnItem.Array = array
			dstArray, _ := col.GetBool("dstArray")
			columnItem.DstArray = dstArray

			jsonArray, _ := col.GetBool("jsonArray")
			columnItem.JsonArray = jsonArray

			field := make(map[string]interface{})
			field["type"] = colTypeStr
			doc_values, _ := col.GetBool("doc_values")
			field["doc_values"] = doc_values
			// TODO ignore_above, _ := col.GetInt64("ignore_above")
			// field["ignore_above"] = ignore_above
			inx, _ := col.GetBool("index")
			field["index"] = inx
			switch colType {
			case STRING:
				// 兼容string类型,ES5之前版本
			case KEYWORD:
				eager_global_ordinals, _ := col.GetBool("eager_global_ordinals")
				field["eager_global_ordinals"] = eager_global_ordinals
			case TEXT:
				analyzer, _ := col.GetString("analyzer")
				if analyzer != "" {
					field["analyzer"] = analyzer
				}
				// 优化disk使用,也同步会提高index性能
				norms, _ := col.GetBool("norms")
				field["norms"] = norms
				// TODO index_options, _ := col.GetBool("index_options")
				// field["index_options"] = index_options
				fds, err := col.GetString("fields")
				if err == nil && fds != "" {
					field["fields"] = fds
				}
			case DATE:
				origin, _ := col.GetBool("origin")
				if origin {
					formart, err := col.GetString("format")
					if err == nil {
						field["format"] = formart
					}
					// es原生format覆盖原先来的format
					dstFormat, err := col.GetString("dstFormat")
					if err == nil {
						field["format"] = dstFormat
					}
					columnItem.Origin = origin
				} else {
					timezone, _ := col.GetString("timezone")
					columnItem.Timezone = timezone
					formart, _ := col.GetString("format")
					columnItem.Format = formart
				}
			case GEO_SHAPE:
				tree, _ := col.GetString("tree")
				field["tree"] = tree
				precision, _ := col.GetString("precision")
				field["precision"] = precision
			case OBJECT:
			case NESTED:
				dynamic, err := col.GetString("dynamic")
				if err == nil {
					field["dynamic"] = dynamic
				}
			default:
			}
			other_params, err := col.GetString("other_params")
			if err == nil && other_params != "" {
				var obj2 map[string]interface{}
				// 解码JSON2到obj2
				json.Unmarshal([]byte(other_params), &obj2)
				// 合并两个对象
				for k, v := range obj2 {
					field[k] = v
				}
			}
			propMap[colName] = field
			columnList = append(columnList, *columnItem)
		}
	}

	version := time.Now().Unix()
	slog.Info(fmt.Sprintf("unified version: %v", version))
	conf.Set("version", version)
	colJson, _ := json.Marshal(columnList)
	conf.Set(WRITE_COLUMNS, colJson)

	rootMappings := make(map[string]interface{})
	typeMappings := make(map[string]interface{})
	typeMappings["properties"] = propMap
	rootMappings[typeName] = typeMappings

	// 7.x以后版本取消了index中关于type的指定，所以mapping的格式只能支持
	// {
	//      "properties" : {
	//          "abc" : {
	//              "type" : "text"
	//              }
	//           }
	// }
	// properties 外不能再嵌套typeName

	if dstDynamic != "" {
		typeMappings["dynamic"] = dstDynamic
	}
	var jbyte []byte
	if isGreaterOrEqualThan7 {
		jbyte, _ = json.Marshal(typeMappings)
		mappings = string(jbyte)
	} else {
		jbyte, _ = json.Marshal(rootMappings)
		mappings = string(jbyte)
	}
	if mappings == "" {
		message := "must have mappings"
		slog.Error(message)
		os.Exit(1)
		return ""
	}

	return mappings
}
