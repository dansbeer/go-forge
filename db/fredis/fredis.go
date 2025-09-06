package fredis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/logrusorgru/aurora"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	URI            string
	Password       string
	DB             int
	ctx            context.Context
	useSentry      bool
	keyPrefix      string
	CustomTimeout  int
	CustomProtocol int
}

const (
	FOLDER_DELIMITER = ":"
)

func NewRedis(uri, db string) (res *Redis, err error) {
	return NewRedisWithPassword(uri, db, "")
}

func NewRedisByOption(db string, opt Redis) (res *Redis, err error) {
	if db == "" {
		err = errors.New("db can't empty")
		return
	}
	dbAsNum, err := strconv.ParseInt(db, 10, 32)
	if err != nil {
		log.Println(aurora.Red(err))
		return
	}

	opt.DB = int(dbAsNum)
	opt.ctx = context.Background()
	res = &opt

	res.Get("", nil) //? Initialize the first connection.
	return
}

func NewRedisWithPasswordWithCustomTimeout(uri, db, password string, customTimeout int) (res *Redis, err error) {
	if db == "" {
		err = errors.New("db can't empty")
		return
	}
	dbAsNum, err := strconv.ParseInt(db, 10, 32)
	if err != nil {
		log.Println(aurora.Red(err))
		return
	}

	res = &Redis{
		URI:           uri,
		Password:      password,
		DB:            int(dbAsNum),
		ctx:           context.Background(),
		CustomTimeout: customTimeout,
	}

	res.Get("", nil) //? Initialize the first connection.
	return
}

func NewRedisWithPassword(uri, db, password string) (res *Redis, err error) {
	return NewRedisWithPasswordWithCustomTimeout(uri, db, password, 0)
}

func NewRedisCacheConnection() (res *Redis, err error) {
	return NewRedisWithPassword(os.Getenv("REDIS_SERVER"), os.Getenv("REDIS_DB_CACHE"), os.Getenv("REDIS_AUTH"))
}

func _NewRedis() (res *Redis, err error) {
	return NewRedis("localhost:6379", "0")
}

var (
	listClient map[any]*redis.Client
	mutex      sync.RWMutex
)

func init() {
	listClient = make(map[any]*redis.Client)
}

func (r *Redis) SetKeyPrefix(keyPrefix string) *Redis {
	r.keyPrefix = keyPrefix
	return r
}

func (o *Redis) SetSentryCtx(ctx context.Context) *Redis {
	o.ctx, o.useSentry = ctx, true
	return o
}

func (o *Redis) setClient(newClient *redis.Client) {
	mutex.Lock()
	listClient[o] = newClient
	mutex.Unlock()

	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		<-sc
		log.Println("Closing client.")
		listClient[o].Close()
		os.Exit(0)
	}()
}

func (o *Redis) getClient() (res *redis.Client) {
	mutex.RLock()
	client := listClient[o]
	mutex.RUnlock()

	if client != nil {
		res = client
		return res
	}
	return
}

func (o *Redis) open() (rdb *redis.Client) {
	if o.useSentry {
		span := sentry.StartSpan(o.ctx, "Redis.open")
		defer span.Finish()
	}

	if rdb = o.getClient(); rdb != nil {
		return
	}
	timeout := 1
	if o.CustomTimeout > int(0) {
		timeout = o.CustomTimeout
	}
	connectionOption := &redis.Options{
		Addr:         o.URI,
		DB:           o.DB,
		PoolSize:     100,
		DialTimeout:  time.Duration(timeout) * time.Second,
		ReadTimeout:  time.Duration(timeout) * time.Second,
		WriteTimeout: time.Duration(timeout) * time.Second,
		Protocol:     2,
	}
	if o.CustomProtocol > 0 { //? example : NewRedisByOption("1", Redis{CustomProtocol: 3})
		connectionOption.Protocol = o.CustomProtocol
	}
	if o.Password != "" {
		connectionOption.Password = o.Password
	}
	rdb = redis.NewClient(connectionOption)
	o.setClient(rdb)
	log.Println(aurora.BrightRed("redis.open"))
	return
}

func (r *Redis) GetClient() (redis *redis.Client) {
	if r == nil {
		log.Println("r nil")
		return nil
	}

	rdb := r.open()
	return rdb
}

func (r *Redis) Set(key string, value any, expireDate time.Duration) (err error) {
	rdb := r.open()
	// defer rdb.Close()

	if r.keyPrefix != "" {
		key = r.keyPrefix + FOLDER_DELIMITER + key
	}
	log.Println(aurora.BrightRed(fmt.Sprintf("redis.set: key=%s", key)))
	status := rdb.Set(r.ctx, key, value, expireDate)
	if err = status.Err(); err != nil {
		if strings.Contains(err.Error(), "(implement encoding.BinaryMarshaler)") {
			err = r.MarshalValueThenSet(key, value, expireDate)
		} else {
			log.Println(aurora.Red(err))
			return
		}
	}
	return
}

func BuildKey(key ...string) string {
	return strings.Join(key, FOLDER_DELIMITER)
}

func (o *Redis) MarshalValueThenSet(key string, value any, expireDate time.Duration) (err error) {
	asJson, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return
	}
	err = o.Set(key, asJson, expireDate)
	return
}

func (r *Redis) Get(key string, ptrDecodeTo any) (found bool) {
	rdb := r.open()
	// defer rdb.Close()

	if r.keyPrefix != "" {
		key = r.keyPrefix + FOLDER_DELIMITER + key
	}
	log.Println(aurora.BrightRed(fmt.Sprintf("\nredis.get: key=%s", aurora.BrightBlue(key))))
	status := rdb.Get(r.ctx, key) //? Only the first execution can take > 150ms time, after that only < 20ms
	if err := status.Err(); err != nil {
		log.Println(key, aurora.Red(err))
		return
	}

	res, err := status.Result()
	if err != nil {
		log.Println(aurora.Red(err))
		return
	}

	switch target := ptrDecodeTo.(type) {
	case *string:
		*target = res
	case *[]byte:
		*target = []byte(res)
	case *bytes.Buffer:
		*target = *bytes.NewBuffer([]byte(res))
	default:
		if err = json.Unmarshal([]byte(res), ptrDecodeTo); err != nil {
			log.Println(aurora.Red(err))
			return
		}
	}

	found = true
	return
}

func (r *Redis) BaseCountAndGetKey(keyPattern string, getWithValue bool) (res int, listKey []string, listValue []any) {
	if r.useSentry {
		span := sentry.StartSpan(r.ctx, "Redis.CountAndGetKey")
		defer span.Finish()
	}
	rdb := r.open()
	// defer rdb.Close()

	if r.keyPrefix != "" {
		keyPattern = r.keyPrefix + FOLDER_DELIMITER + keyPattern
	}
	iter := rdb.Scan(r.ctx, 0, keyPattern, 0).Iterator()
	for iter.Next(r.ctx) {
		listKey = append(listKey, iter.Val())
	}
	if err := iter.Err(); err != nil {
		log.Println(err)
		return
	}

	if getWithValue {
		mgetRes := rdb.MGet(r.ctx, listKey...)
		listKey = []string{}
		if err := mgetRes.Err(); err != nil {
			log.Println(err)
			return
		}
		for _, each := range mgetRes.Args() {
			asStr, _ := each.(string)
			if asStr == "mget" {
				continue
			}

			listKey = append(listKey, asStr)
		}
		listValue = append(listValue, mgetRes.Val()...)
	}

	res = len(listKey)
	return
}

func (r *Redis) CountAndGetKeyAndValue(keyPattern string) (res int, listKey []string, listValue []any) {
	res, listKey, listValue = r.BaseCountAndGetKey(keyPattern, true)
	return
}

func (r *Redis) CountAndGetKey(keyPattern string) (res int, listKey []string) {
	res, listKey, _ = r.BaseCountAndGetKey(keyPattern, false)
	return
}

func (o *Redis) Count(keyPattern string) (res int) {
	res, _ = o.CountAndGetKey(keyPattern)
	return
}

func (r *Redis) Delete(key string) (err error) {
	err = r.Set(key, "", 1*time.Millisecond)
	return
}

func (r *Redis) DeleteByPattern(pattern string) (listDeletedKey []string, err error) {
	rdb := r.open()

	listDeletedKey, err = rdb.Keys(r.ctx, pattern).Result()
	if err != nil {
		log.Println(err)
		return
	}

	err = rdb.Del(r.ctx, listDeletedKey...).Err()
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println("Deleted keys:", len(listDeletedKey))
	return
}
