package fsql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/dansbeer/go-forge/db/fredis"
	"github.com/getsentry/sentry-go"
	"github.com/logrusorgru/aurora"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/postgres"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type SQLConn_Type int

const (
	SQLConn_Type_Clickhouse SQLConn_Type = iota
	SQLConn_Type_Postgre
)

func (index SQLConn_Type) String() string {
	return []string{
		"clickhouse",
		"postgre",
	}[index]
}

type SQLConn struct {
	Host           string
	Db             string
	User           string
	Pwd            string
	dsn            string
	ConnectionType SQLConn_Type
	disableCache   bool
	redisConn      *fredis.Redis
}

func BaseNewClickHouseFromEnv(host, db, user, pwd string) *SQLConn {
	sqlConn := SQLConn{
		Host:           host,
		Db:             db,
		User:           user,
		Pwd:            pwd,
		ConnectionType: SQLConn_Type_Clickhouse,
	}
	sqlConn.dsn = fmt.Sprintf("clickhouse://%s:%s@%s/%s?read_timeout=30s", sqlConn.User, sqlConn.Pwd, sqlConn.Host, sqlConn.Db)
	return &sqlConn
}

func NewClickHouseFromEnv() *SQLConn {
	return BaseNewClickHouseFromEnv(os.Getenv("CH_HOST"), os.Getenv("CH_DB"), os.Getenv("CH_USER"), os.Getenv("CH_PWD"))
}

func BaseNewPostgreFromEnv(host, db, user, pwd string) *SQLConn {
	sqlConn := SQLConn{
		Host:           host,
		Db:             db,
		User:           user,
		Pwd:            pwd,
		ConnectionType: SQLConn_Type_Postgre,
	}
	splitHost := strings.Split(sqlConn.Host, ":")
	sqlConn.dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		splitHost[0], splitHost[1], sqlConn.User, sqlConn.Pwd, sqlConn.Db)
	return &sqlConn
}

func NewPostgreFromEnv() *SQLConn {
	return BaseNewPostgreFromEnv(os.Getenv("POSTGRE_BT_HOST"), os.Getenv("POSTGRE_BT_DB"), os.Getenv("POSTGRE_BT_USER"), os.Getenv("POSTGRE_BT_PWD"))
}

func NewSQLConn(connectionType SQLConn_Type) (res *SQLConn) {
	switch connectionType {
	case SQLConn_Type_Clickhouse:
		res = NewClickHouseFromEnv()
	case SQLConn_Type_Postgre:
		res = NewPostgreFromEnv()
	}
	return
}

func (sc *SQLConn) SetDisableCache(disableCache bool) *SQLConn {
	sc.disableCache = disableCache
	return sc
}

func (sc *SQLConn) SetRedisConn(redisConn *fredis.Redis) *SQLConn {
	sc.redisConn = redisConn
	return sc
}

func (sc *SQLConn) connect() (db *gorm.DB, err error) {
	if sc.ConnectionType == SQLConn_Type_Clickhouse {
		db, err = gorm.Open(clickhouse.Open(sc.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Info)})
		if err != nil {
			fmt.Printf("o: %+v\n", sc)
			if strings.Contains(err.Error(), "i/o timeout") {
				return sc.connect()
			}
			return
		}
	} else if sc.ConnectionType == SQLConn_Type_Postgre {
		db, err = gorm.Open(postgres.Open(sc.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Info)})
		if err != nil {
			fmt.Printf("o: %+v\n", sc)
			return
		}
	}
	return
}

func close(db *gorm.DB) {
	dbClient, err := db.DB()
	if err != nil {
		log.Println(err)
		return
	}
	dbClient.Close()
}

func (sc *SQLConn) BaseExec(query string, ptrDecodeTo interface{}) (err error) {
	log.Println("Executing Query: \n", aurora.Magenta(query))

	cacheKey := fmt.Sprintf("%s__%s", sc.ConnectionType, query)
	var redis *fredis.Redis
	if sc.redisConn == nil {
		redis, err = fredis.NewRedisCacheConnection()
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		redis = sc.redisConn
	}

	if found := redis.Get(cacheKey, ptrDecodeTo); !found || sc.disableCache {
		var db *gorm.DB
		db, err = sc.connect()
		if err != nil {
			log.Println(err)
			return
		}
		defer close(db)

		if queryRes := db.Raw(query).Scan(ptrDecodeTo); queryRes.Error != nil {
			fmt.Println(aurora.Yellow(query))
			log.Println(queryRes.Error)
			return
		}

		if !sc.disableCache {
			res, _ := json.Marshal(ptrDecodeTo)
			err = redis.Set(cacheKey, string(res), 6*time.Hour)
			if err != nil {
				log.Println(err)
			}
			redis.Get(cacheKey, ptrDecodeTo) //? To prevent different type data from DB SQL & Redis, always return only from Redis
		}
	} else {
		fmt.Println(aurora.Red("CACHED"))
	}
	return
}

func (sc *SQLConn) Exec(query string) (res []byte, err error) {
	var data []map[string]any
	query = strings.ReplaceAll(query, "\n", " ")
	if err = sc.BaseExec(query, &data); err != nil {
		log.Println(err)
		return
	}

	res, err = json.Marshal(data)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

func (sc *SQLConn) Insert(ptrStruct any) (err error) {
	var db *gorm.DB
	db, err = sc.connect()
	if err != nil {
		log.Println(err)
		return
	}
	defer close(db)

	db.Create(ptrStruct)
	return
}

// ? don't forget to alias the field to 'total'
func (sc *SQLConn) GetCount(query *bytes.Buffer) (res float64, err error) {
	data := []map[string]any{}
	if err = sc.BaseExec(query.String(), &data); err != nil && !strings.Contains(err.Error(), context.Canceled.Error()) {
		log.Println(err)
		sentry.CaptureException(err)
		return
	}

	for _, datum := range data {
		if total, found := datum["total"]; found {
			switch asNum := total.(type) {
			case uint64:
				res = float64(asNum)
			case float64:
				res = asNum
			case int64:
				res = float64(asNum)
			default:
				log.Printf("DEFAULT: %T", asNum)
			}
			return
		}
	}
	return
}
