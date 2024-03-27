package elasticsearch

import (
	"strings"

	"github.com/as-tool/as-etl-engine/common/config"
)

// const (
// 	ID            = "ID"
// 	PARENT        = "PARENT"
// 	ROUTING       = "ROUTING"
// 	VERSION       = "VERSION"
// 	STRING        = "STRING"
// 	TEXT          = "TEXT"
// 	KEYWORD       = "KEYWORD"
// 	LONG          = "LONG"
// 	INTEGER       = "INTEGER"
// 	SHORT         = "SHORT"
// 	BYTE          = "BYTE"
// 	DOUBLE        = "DOUBLE"
// 	FLOAT         = "FLOAT"
// 	DATE          = "DATE"
// 	BOOLEAN       = "BOOLEAN"
// 	BINARY        = "BINARY"
// 	INTEGER_RANGE = "INTEGER_RANGE"
// 	FLOAT_RANGE   = "FLOAT_RANGE"
// 	LONG_RANGE    = "LONG_RANGE"
// 	DOUBLE_RANGE  = "DOUBLE_RANGE"
// 	DATE_RANGE    = "DATE_RANGE"
// 	GEO_POINT     = "GEO_POINT"
// 	GEO_SHAPE     = "GEO_SHAPE"
// 	IP            = "IP"
// 	IP_RANGE      = "IP_RANGE"
// 	COMPLETION    = "COMPLETION"
// 	TOKEN_COUNT   = "TOKEN_COUNT"
// 	OBJECT        = "OBJECT"
// 	NESTED        = "NESTED"
// )

// const (
// 	UNKNOW = "UNKNOW"
// 	INDEX  = "INDEX"
// 	CREATE = "CREATE"
// 	DELETE = "DELETE"
// 	UPDATE = "UPDATE"
// )

// 定义ActionType常量
const (
	UNKNOW ActionType = iota
	INDEX
	CREATE
	DELETE
	UPDATE
)

// 定义ActionType类型
type ActionType int

// String方法返回ActionType的字符串表示
func (at ActionType) String() string {
	switch at {
	case UNKNOW:
		return "UNKNOWN"
	case INDEX:
		return "INDEX"
	case CREATE:
		return "CREATE"
	case DELETE:
		return "DELETE"
	case UPDATE:
		return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

func GetActionType(conf *config.JSON) ActionType {
	actionType, err := conf.GetString("actionType")
	if actionType == "" || err != nil {
		actionType = "index"
	}
	actionType = strings.ToUpper(actionType)
	switch actionType {
	case "INDEX":
		return INDEX
	case "CREATE":
		return CREATE
	case "DELETE":
		return DELETE
	case "UPDATE":
		return UPDATE
	default:
		return UNKNOW
	}
}

// ElasticSearchFieldType 枚举类型
type ElasticSearchFieldType int

const (
	ID ElasticSearchFieldType = iota
	PARENT
	ROUTING
	VERSION
	STRING
	TEXT
	KEYWORD
	LONG
	INTEGER
	SHORT
	BYTE
	DOUBLE
	FLOAT
	DATE
	BOOLEAN
	BINARY
	INTEGER_RANGE
	FLOAT_RANGE
	LONG_RANGE
	DOUBLE_RANGE
	DATE_RANGE
	GEO_POINT
	GEO_SHAPE
	IP
	IP_RANGE
	COMPLETION
	TOKEN_COUNT
	OBJECT
	NESTED
)

// String 方法用于返回ElasticSearchFieldType的字符串表示
func (f ElasticSearchFieldType) String() string {
	switch f {
	case ID:
		return "ID"
	case PARENT:
		return "PARENT"
	case ROUTING:
		return "ROUTING"
	case VERSION:
		return "VERSION"
	case STRING:
		return "STRING"
	case TEXT:
		return "TEXT"
	case KEYWORD:
		return "KEYWORD"
	case LONG:
		return "LONG"
	case INTEGER:
		return "INTEGER"
	case SHORT:
		return "SHORT"
	case BYTE:
		return "BYTE"
	case DOUBLE:
		return "DOUBLE"
	case FLOAT:
		return "FLOAT"
	case DATE:
		return "DATE"
	case BOOLEAN:
		return "BOOLEAN"
	case BINARY:
		return "BINARY"
	case INTEGER_RANGE:
		return "INTEGER_RANGE"
	case FLOAT_RANGE:
		return "FLOAT_RANGE"
	case LONG_RANGE:
		return "LONG_RANGE"
	case DOUBLE_RANGE:
		return "DOUBLE_RANGE"
	case DATE_RANGE:
		return "DATE_RANGE"
	case GEO_POINT:
		return "GEO_POINT"
	case GEO_SHAPE:
		return "GEO_SHAPE"
	case IP:
		return "IP"
	case IP_RANGE:
		return "IP_RANGE"
	case COMPLETION:
		return "COMPLETION"
	case TOKEN_COUNT:
		return "TOKEN_COUNT"
	case OBJECT:
		return "OBJECT"
	case NESTED:
		return "NESTED"
	default:
		return "Unknown"
	}
}

// getESFieldType 根据传入的字符串类型返回对应的ElasticSearchFieldType
func GetESFieldType(typeStr string) ElasticSearchFieldType {
	typeStr = strings.ToUpper(typeStr)
	switch typeStr {
	case "ID":
		return ID
	case "PARENT":
		return PARENT
	case "ROUTING":
		return ROUTING
	case "VERSION":
		return VERSION
	case "STRING":
		return STRING
	case "TEXT":
		return TEXT
	case "KEYWORD":
		return KEYWORD
	case "LONG":
		return LONG
	case "INTEGER":
		return INTEGER
	case "SHORT":
		return SHORT
	case "BYTE":
		return BYTE
	case "DOUBLE":
		return DOUBLE
	case "FLOAT":
		return FLOAT
	case "DATE":
		return DATE
	case "BOOLEAN":
		return BOOLEAN
	case "BINARY":
		return BINARY
	case "INTEGER_RANGE":
		return INTEGER_RANGE
	case "FLOAT_RANGE":
		return FLOAT_RANGE
	case "LONG_RANGE":
		return LONG_RANGE
	case "DOUBLE_RANGE":
		return DOUBLE_RANGE
	case "DATE_RANGE":
		return DATE_RANGE
	case "GEO_POINT":
		return GEO_POINT
	case "GEO_SHAPE":
		return GEO_SHAPE
	case "IP":
		return IP
	case "IP_RANGE":
		return IP_RANGE
	case "COMPLETION":
		return COMPLETION
	case "TOKEN_COUNT":
		return TOKEN_COUNT
	case "OBJECT":
		return OBJECT
	case "NESTED":
		return NESTED
	default:
		return -1 // 或者定义一个新的常量来表示未知类型
	}
}
