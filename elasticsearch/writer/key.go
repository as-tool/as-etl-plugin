package elasticsearch

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/common/encoding"
	"github.com/olivere/elastic/v7"
)

const (
	WRITE_COLUMNS = "write_columns"
)

func GetEndpoint(conf *config.JSON) string {
	v, _ := conf.GetString("endpoint")
	return v
}

func GetUsername(conf *config.JSON) string {
	username, _ := conf.GetString("username")
	return username
}

func GetPassword(conf *config.JSON) string {
	pass, _ := conf.GetString("password")
	return pass
}

func GetBatchSize(conf *config.JSON) int64 {
	v, _ := conf.GetInt64("batchSize")
	return v
}

func IsDiscovery(conf *config.JSON) bool {
	v, _ := conf.GetBool("discovery")
	return v
}

func GetTrySize(conf *config.JSON) int64 {
	v, _ := conf.GetInt64("trySize")
	if v == 0 {
		return 30
	}
	return v
}

func GetTryInterval(conf *config.JSON) int64 {
	v, _ := conf.GetInt64("tryInterval")
	if v == 0 {
		return 60000
	}
	return v
}

func GetTimeout(conf *config.JSON) int64 {
	v, _ := conf.GetInt64("timeout")
	return v
}

func IsTruncate(conf *config.JSON) bool {
	v, err := conf.GetBool("truncate")
	if err != nil {
		f, err := conf.GetBool("cleanup")
		if err != nil {
			return false
		}
		return f
	}
	return v
}

func IsCompression(conf *config.JSON) bool {
	v, err := conf.GetBool("compress")
	if err != nil {
		f, _ := conf.GetBool("compression")
		return f
	}
	return v
}

func IsMultiThread(conf *config.JSON) bool {
	v, err := conf.GetBool("multiThread")
	if err != nil {

		return true
	}
	return v
}

func GetIndexName(conf *config.JSON) string {
	v, err := conf.GetString("index")
	if err != nil {
		return "The value you configured is not valid"
	}
	return v
}

func GetDeleteBy(conf *config.JSON) string {
	v, _ := conf.GetString("deleteBy")
	return v
}

func GetTypeName(conf *config.JSON) string {
	v, err := conf.GetString("indexType")
	if v == "" || err != nil {
		indexType, err2 := conf.GetString("type")
		if err2 != nil {
			return GetIndexName(conf)
		}
		return indexType
	}
	return v
}

func IsIgnoreWriteError(conf *config.JSON) bool {
	v, err := conf.GetBool("ignoreWriteError")
	if err != nil {

		return false
	}
	return v
}

func IsIgnoreParseError(conf *config.JSON) bool {
	v, err := conf.GetBool("ignoreParseError")
	if err != nil {

		return true
	}
	return v
}

func IsHighSpeedMode(conf *config.JSON) bool {
	v, err := conf.GetString("mode")
	if err != nil {
		return false
	}
	return v == "highspeed"
}

func GetAlias(conf *config.JSON) string {
	v, _ := conf.GetString("alias")
	return v
}

func IsNeedCleanAlias(conf *config.JSON) bool {
	v, err := conf.GetString("aliasMode")
	if err != nil {
		v = "append"
	}
	return v == "exclusive"
}

func GetSplitter(conf *config.JSON) string {
	v, err := conf.GetString("splitter")
	if v == "" || err != nil {
		return "-,-"
	}
	return v
}

func GetDynamic(conf *config.JSON) bool {
	v, err := conf.GetBool("dynamic")
	if err != nil {
		return false
	}
	return v
}

func GetDstDynamic(conf *config.JSON) string {
	v, _ := conf.GetString("dstDynamic")
	return v
}

func GetDiscoveryFilter(conf *config.JSON) string {
	v, err := conf.GetString("discoveryFilter")
	if v == "" || err != nil {
		return "_all"
	}
	return v
}

func GetVersioning(conf *config.JSON) bool {
	v, err := conf.GetBool("versioning")
	if err != nil {
		return false
	}
	return v
}

func GetUnifiedVersion(conf *config.JSON) int64 {
	version, err := conf.GetInt64("version")
	if version == 0 || err != nil {
		return time.Now().UnixNano() / int64(time.Millisecond)
	}
	return version
}

func GetUrlParams(conf *config.JSON) map[string]interface{} {
	mapObj, err := conf.GetMap("urlParams")
	newMap := make(map[string]interface{})
	if err != nil {
		return newMap
	}
	for k, v := range mapObj {
		newMap[k] = v
	}
	return newMap
}

func GetESVersion(conf *config.JSON) int64 {
	v, _ := conf.GetInt64("esVersion")
	return v
}

func GetMasterTimeout(conf *config.JSON) string {
	v, err := conf.GetString("masterTimeout")
	if v == "" || err != nil {
		return "5m"
	}
	return v
}

func IsEnableNullUpdate(conf *config.JSON) bool {
	v, err := conf.GetBool("enableWriteNull")
	if err != nil {
		return true
	}
	return v
}

func GetFieldDelimiter(conf *config.JSON) string {
	v, _ := conf.GetString("fieldDelimiter")
	return v
}

func GetSettings(conf *config.JSON) map[string]*encoding.JSON {
	v, err := conf.GetMap("settings")
	if err != nil {
		return make(map[string]*encoding.JSON)
	}
	return v
}

func GetPrimaryKeyInfo(conf *config.JSON) *PrimaryKeyInfo {
	primaryKeyInfoString, _ := conf.GetString("primaryKeyInfo")
	if primaryKeyInfoString != "" {
		t := &PrimaryKeyInfo{}
		json.Unmarshal([]byte(primaryKeyInfoString), t)
		return t
	} else {
		return nil
	}
}

func GetEsPartitionColumn(conf *config.JSON) []PartitionColumn {

	esPartitionColumnString, _ := conf.GetString("esPartitionColumn")
	if esPartitionColumnString != "" {
		var col []PartitionColumn
		json.Unmarshal([]byte(esPartitionColumnString), &col)
		return col
	} else {
		return nil
	}
}

func getEnableRedundantColumn(conf *config.JSON) bool {
	v, err := conf.GetBool("enableRedundantColumn")
	if err != nil {
		return false
	}
	return v
}

func GetColumnList(conf *config.JSON) []EsColumn {
	arr, err := conf.GetArray("column")
	if err != nil {
		return nil
	}
	var cols = make([]EsColumn, 0)
	for _, v := range arr {
		t, err := v.GetString("type")
		if err != nil {
			return nil
		}
		n, err := v.GetString("name")
		if err != nil {
			return nil
		}
		var col = &EsColumn{
			Type: strings.ToUpper(t),
			Name: n,
		}
		cols = append(cols, *col)
	}

	return cols

}

func GetWriteColumns(conf *config.JSON) []EsColumn {
	arr, err := conf.GetString(WRITE_COLUMNS)
	if err != nil {
		return nil
	}
	var cols []EsColumn
	json.Unmarshal([]byte(arr), &cols)
	return cols

}

func GetcombinedIdColumn(columnList []EsColumn, typeList *[]string) *EsColumn {
	for _, esColumn := range columnList {
		_type := GetESFieldType(esColumn.Type).String()
		*typeList = append(*typeList, _type)
		if len(esColumn.CombineFields) > 0 && GetESFieldType(esColumn.Type) == ID {
			return &esColumn
		}
	}
	return nil
}

func HasID(conf *config.JSON) bool {
	cols := GetColumnList(conf)
	for _, col := range cols {
		colTypeStr := col.Type
		colType := GetESFieldType(colTypeStr)
		if ID == colType {
			return true
		}
	}
	return false
}

func HasPrimaryKeyInfo(conf *config.JSON) bool {
	primaryKeyInfo := GetPrimaryKeyInfo(conf)
	if nil != primaryKeyInfo && nil != primaryKeyInfo.Column {
		return true
	} else {
		return false
	}
}

func GetIncludeSettings(conf *config.JSON) []string {
	arr, err := conf.GetArray("includeSettingKeys")
	if len(arr) == 0 || err != nil {
		return []string{"number_of_shards", "number_of_replicas"}
	}
	return []string{}
}

func GetSleepTimeInMilliSecond(conf *config.JSON) int64 {
	sd, err := conf.GetInt64("sleepTimeInMilliSecond")
	if sd == 0 || err != nil {
		sd = 10000
	}
	return sd
}

func GetRetryTimes(conf *config.JSON) int64 {
	rs, err := conf.GetInt64("retryTimes")
	if rs == 0 || err != nil {
		rs = 3
	}
	return rs
}

func IsGreaterOrEqualThan7(conf *config.JSON, client *elastic.Client) bool {
	esVersion := GetESVersion(conf)
	var vint int64
	if client != nil {
		version := ES_Version(client, conf)
		vs := strings.Split(version, ".")[0]
		vint, _ = strconv.ParseInt(vs, 10, 64)
	}

	if esVersion >= 7 || vint >= 7 {
		return true
	}
	return false
}

func ParseDeleteCondition(conf *config.JSON) []map[string]interface{} {
	var list []map[string]interface{}
	deletes := GetDeleteBy(conf)
	json.Unmarshal([]byte(deletes), &list)
	return list
}

func IsHasId(conf *config.JSON) bool {
	v, err := conf.GetBool("hasId")
	if err != nil {
		return false
	}
	return v
}
