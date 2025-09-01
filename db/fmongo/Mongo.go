package fmongo

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"go-force/fstring/fid"
	"log"
	"math"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/logrusorgru/aurora"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/exp/slices"
)

type MongoDbUtil struct {
	Srv            string
	DbName         string
	CollectionName string
	Ctx            context.Context
	useSentry      bool

	projection                 bson.M
	customDefaultFilter        bson.M
	disableFilterStatusArchive bool
}

func (umongo *MongoDbUtil) SetDisableFilterStatusArchive(disableFilterStatusArchive bool) *MongoDbUtil {
	umongo.disableFilterStatusArchive = disableFilterStatusArchive
	return umongo
}

func (umongo *MongoDbUtil) SetProjection(projection bson.M) *MongoDbUtil {
	umongo.projection = projection
	return umongo
}

func (umongo *MongoDbUtil) SetCol(col string) *MongoDbUtil {
	umongo.CollectionName = col
	return umongo
}

func NewMongoDbUtil(srv, dbName, collectionName string) *MongoDbUtil {
	return &MongoDbUtil{
		Srv:            srv,
		DbName:         dbName,
		CollectionName: collectionName,
		Ctx:            context.Background(),
	}
}

func NewMongoDbUtilGlobalCol(srv, dbName string) *MongoDbUtil {
	return &MongoDbUtil{
		Srv:    srv,
		DbName: dbName,
		Ctx:    context.Background(),
	}
}

func NewMongoDbUtilByStruct(newStruct MongoDbUtil) *MongoDbUtil {
	return &newStruct
}

func NewMongoDbUtilUseEnv(collectionName string) *MongoDbUtil {
	return &MongoDbUtil{
		Srv:            os.Getenv("DB_MONGO_SRV"),
		DbName:         os.Getenv("DB_NAME"),
		CollectionName: collectionName,
		Ctx:            context.Background(),
	}
}

func NewMongoDbUtilUseEnvCustomCtx(collectionName string, ctx context.Context) *MongoDbUtil {
	return &MongoDbUtil{
		Srv:            os.Getenv("DB_MONGO_SRV"),
		DbName:         os.Getenv("DB_NAME"),
		CollectionName: collectionName,
		Ctx:            ctx,
	}
}

func InitConnection(srvKey, dbName string) {
	err := NewMongoDbUtil(os.Getenv(srvKey), os.Getenv(dbName), "").
		BaseFindOne(bson.M{}, nil)
	if err != nil {
		log.Println(err)
		return
	} //? Heat up the connection.
}

func (umongo *MongoDbUtil) SetSentryCtx(ctx context.Context) *MongoDbUtil {
	umongo.Ctx, umongo.useSentry = ctx, true
	return umongo
}

func (umongo *MongoDbUtil) GetDatabase() (client *mongo.Client, db *mongo.Database) {
	client, err := umongo.connect()
	if err != nil {
		return
	}

	db = client.Database(umongo.DbName, &options.DatabaseOptions{})
	return
}

func (umongo *MongoDbUtil) GetCollection() (client *mongo.Client, col *mongo.Collection) {
	client, db := umongo.GetDatabase()
	col = db.Collection(umongo.CollectionName)
	return
}

var (
	client sync.Map
	mutex  sync.RWMutex
)

func (umongo *MongoDbUtil) getClient() (res *mongo.Client) {
	value, ok := client.Load(umongo.generateStoreKey())
	if !ok {
		log.Println("no client", umongo.DbName)
		return
	}
	asClient, ok := value.(*mongo.Client)
	if !ok {
		log.Printf("%T", value)
		return
	}
	res = asClient
	return
}

func (umongo *MongoDbUtil) generateStoreKey() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(umongo.Srv+umongo.DbName)))
}

func (umongo *MongoDbUtil) setClient(newClient *mongo.Client) *mongo.Client {
	mutex.Lock()
	defer mutex.Unlock()

	client.Store(umongo.generateStoreKey(), newClient)

	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		<-sc
		log.Println("Closing client")
		err := umongo.getClient().Disconnect(context.Background())
		if err != nil {
			log.Println(err)
			return
		}
		os.Exit(0)
	}()
	return umongo.getClient()
}

func (umongo *MongoDbUtil) connect() (newClient *mongo.Client, err error) {
	if client := umongo.getClient(); client != nil {
		return client, err
	}

	clientOptions := options.Client()
	clientOptions.ApplyURI(umongo.Srv)
	clientOptions.SetMaxPoolSize(200)

	fmt.Println(aurora.Green("mongo.Connect"))
	newClient, err = mongo.Connect(umongo.Ctx, clientOptions)
	if err != nil {
		log.Println(err)
		err = errors.New("fail connect to data")
		return
	}

	umongo.setClient(newClient)
	return
}

func (umongo *MongoDbUtil) Disconnect(client *mongo.Client) {
	if client == nil {
		return
	}

	//? Noop, due single open connection
	// if err := client.Disconnect(umongo.ctx); err != nil {
	// 	log.Println(err)
	// 	return
	// }
}

func (umongo *MongoDbUtil) BaseUpdateOne(filter, update bson.M) {
	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	var updateRes *mongo.UpdateResult
	if updateRes, err = col.UpdateOne(umongo.Ctx, filter, update); err != nil {
		log.Println(err)
		return
	}
	log.Printf("updateRes: %+v\n", updateRes)
}

func (umongo *MongoDbUtil) BaseUpdateOneAny(filter bson.M, update any) {
	asMap := bson.M{}
	asJson, err := json.Marshal(update)
	if err != nil {
		log.Println(err)
		return
	}
	if err := json.Unmarshal(asJson, &asMap); err != nil {
		log.Println(err)
		return
	}

	umongo.BaseUpdateOne(filter, bson.M{"$set": asMap})
}

func (umongo *MongoDbUtil) UpsertAndGetId(isUpdate bool, ptrParam interface{}) (newDataId string, err error) {
	newDataId, err = umongo.baseUpsert(isUpdate, ptrParam)
	return
}

func (umongo *MongoDbUtil) Upsert(isUpdate bool, ptrParam interface{}) (err error) {
	_, err = umongo.baseUpsert(isUpdate, ptrParam)
	return
}

func (umongo *MongoDbUtil) defaultCollectionOption() *options.CollectionOptions {
	opts := options.CollectionOptions{
		BSONOptions: &options.BSONOptions{
			DefaultDocumentM: false,
		},
	}
	return &opts
}

func (umongo *MongoDbUtil) baseUpsert(isUpdate bool, ptrParam interface{}) (newDataId string, err error) {
	if umongo.useSentry {
		span := sentry.StartSpan(umongo.Ctx, "MongoDbUtil.Upsert")
		defer span.Finish()
	}

	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)

	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	if reflect.ValueOf(ptrParam).Kind() != reflect.Pointer {
		log.Println("ptrParam is not pointer")
		return
	}
	paramAsReflect := reflect.ValueOf(ptrParam).Elem()
	if paramAsReflect.Kind() == reflect.Map {
		(*ptrParam.(*map[string]any))["updatedAt"] = time.Now().UnixMilli()
	} else {
		if updatedAt := paramAsReflect.FieldByName("UpdatedAt"); updatedAt.IsValid() {
			updatedAt.SetInt(time.Now().UnixMilli())
		}
	}
	IdKey := "IdDocument"
	var idField reflect.Value
	if paramAsReflect.Kind() != reflect.Map {
		idField = paramAsReflect.FieldByName(IdKey)
		if !idField.IsValid() {
			log.Println(aurora.Red("FAIL TO REFLECT _ID FIELD"))
		}
	}

	if !isUpdate {
		if paramAsReflect.Kind() == reflect.Map {
			(*ptrParam.(*map[string]any))["createdAt"] = time.Now().UnixMilli()
		} else {
			if createdAt := paramAsReflect.FieldByName("CreatedAt"); createdAt.IsValid() && createdAt.CanInt() {
				createdAt.SetInt(time.Now().UnixMilli())
			}
			if idField.IsValid() && len(idField.String()) == 0 {
				idField.SetString(fid.GenerateID())
			}
		}

		if paramAsReflect.Kind() == reflect.Map {
			asMap, ok := ptrParam.(*map[string]any)
			if ok {
				newDataId = fid.GenerateID()
				if idExists, ok := (*asMap)["_id"].(string); ok {
					newDataId = idExists
				} else {
					(*ptrParam.(*map[string]any))["_id"] = newDataId
				}
			}
		} else {
			newDataId = idField.String()
		}

		if _, err = col.InsertOne(umongo.Ctx, ptrParam); err != nil {
			log.Println(err)
			if strings.Contains(err.Error(), "_id_ dup key") {
				err = errors.New("data is duplicated")
			} else {
				err = errors.New("fail Add")
			}
			return
		}
	} else {
		var id string
		if paramAsReflect.Kind() == reflect.Map {
			asMap, ok := ptrParam.(*map[string]any)
			if ok {
				id = fmt.Sprint((*asMap)["_id"])
				delete(*asMap, "_id")
				ptrParam = asMap
			}
		} else if !idField.IsValid() {
			log.Println("Fail to get field ID")
			return
		} else {
			id = idField.String()
		}

		var updateRes *mongo.UpdateResult
		if updateRes, err = col.UpdateByID(umongo.Ctx, id, bson.M{"$set": ptrParam}, options.Update().SetUpsert(true)); err != nil {
			log.Println(err)
			return
		} else {
			if updateRes.MatchedCount == 0 && updateRes.UpsertedID == "" {
				err = errors.New("data not found. nothing updated")
				log.Println(err)
				return
			}
			newDataId = id
		}
	}

	return
}

func (umongo *MongoDbUtil) SetCustomDefaultFilter(customDefaultFilter bson.M) *MongoDbUtil {
	umongo.customDefaultFilter = customDefaultFilter
	return umongo
}

func (umongo *MongoDbUtil) defaultFindFilter(filter bson.M) bson.M {
	if len(umongo.customDefaultFilter) > 0 {
		for key, value := range umongo.customDefaultFilter {
			filter[key] = value
		}
	}

	//* ------------------------------- DEFAULT FILTER ------------------------------ */
	filterArchive := bson.M{"$ne": statusArchive}
	if status, ok := filter["status"]; ok {
		if status == statusArchive { //? Condition when need to search data archive
		} else {
			if filter["$and"] == nil {
				filter["$and"] = []bson.M{}
			}
			filter["$and"] = append(filter["$and"].([]bson.M), bson.M{"status": filterArchive})
		}
	} else if !umongo.disableFilterStatusArchive {
		filter["status"] = filterArchive //? Delete operation will set data status to archive instead removing the data.
	}
	return filter
}

func (umongo MongoDbUtil) BaseFindOneMap(filter bson.M) (result interface{}, err error) {
	client, err := umongo.connect()
	var ress bson.D
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	res := col.FindOne(umongo.Ctx, filter)
	if res.Err() != nil {
		fmt.Printf("o: %+v\n", umongo)
		filterAsJson, _ := json.Marshal(filter)
		log.Println(err, umongo.CollectionName, string(filterAsJson))
		err = errors.New("data not found")
		return
	}

	err = res.Decode(&ress)

	result = ress
	if err != nil {
		log.Println(err)
	}

	return
}

func (o *MongoDbUtil) FindOneMap(key, value string) (result interface{}, err error) {
	return o.BaseFindOneMap(bson.M{key: value})
}

func (o *MongoDbUtil) FindOneMapUnique(key, value, key1 string, value1 int64) (result interface{}, err error) {
	return o.BaseFindOneMap(bson.M{key: value, key1: value1})
}

func (umongo MongoDbUtil) BaseFindOne(filter bson.M, pointerDecodeTo interface{}) (err error) {
	if umongo.useSentry {
		span := sentry.StartSpan(umongo.Ctx, "MongoDbUtil.BaseFindOne")
		defer span.Finish()
	}

	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	filter = umongo.defaultFindFilter(filter)
	res := col.FindOne(umongo.Ctx, filter)
	if res.Err() != nil {
		log.Println(res.Err(), filter)
		err = errors.New("data not found")
		return
	}

	if err := res.Decode(pointerDecodeTo); err != nil {
		log.Println(err)
	}
	return
}

func (umongo *MongoDbUtil) FindOne(key, value string, pointerDecodeTo interface{}) (err error) {
	return umongo.BaseFindOne(bson.M{key: value}, pointerDecodeTo)
}

func (umongo *MongoDbUtil) GjsonFindOne(key, value string) (res gjson.Result, err error) {
	pointerDecodeTo := bson.M{}
	if err := umongo.BaseFindOne(bson.M{key: value}, pointerDecodeTo); err != nil {
		return gjson.Result{}, err
	}
	asJson, err := json.Marshal(pointerDecodeTo)
	if err != nil {
		log.Println(err)
		return
	}
	res = gjson.ParseBytes(asJson)
	return
}

func (umongo *MongoDbUtil) GetAggregate(groupStage mongo.Pipeline) (results []bson.M) {
	_, coll := umongo.GetCollection()
	cursor, err := coll.Aggregate(context.Background(), groupStage)
	if err != nil {
		log.Println(err)
		return
	}

	if err = cursor.All(context.Background(), &results); err != nil {
		log.Println(err)
		return
	}
	return
}

func (umongo *MongoDbUtil) BaseFind(filter bson.M, findOptions options.FindOptions, pointerDecodeTo interface{}) (err error) {
	if umongo.useSentry {
		span := sentry.StartSpan(umongo.Ctx, "MongoDbUtil.BaseFind")
		defer span.Finish()
	}

	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	if len(umongo.projection) > 0 {
		findOptions.SetProjection(umongo.projection)
	}

	filter = umongo.defaultFindFilter(filter)
	findRes, err := col.Find(umongo.Ctx, filter, &findOptions)
	if err != nil {
		log.Println(err)
		return
	}
	if findRes.Err() != nil {
		log.Println(err)
		return
	}

	if err = findRes.All(umongo.Ctx, pointerDecodeTo); err != nil {
		log.Println(err)
		return
	}
	return
}

func (umongo *MongoDbUtil) MapToList(includeField []string) (res map[string][]any) {
	var data []bson.M
	resAsMap, res, projection := map[string]map[any]bool{}, map[string][]any{}, bson.M{
		"_id": 0,
	}
	for _, field := range includeField {
		projection[field] = 1
	}

	if err := umongo.BaseFind(bson.M{}, *options.Find().SetProjection(projection), &data); err != nil {
		log.Println(err)
		return
	}

	for _, datum := range data {
		for key, value := range datum {
			if _, ok := resAsMap[key]; !ok {
				resAsMap[key] = map[any]bool{}
			}
			resAsMap[key][value] = true
		}
	}

	for field, list := range resAsMap {
		for value := range list {
			res[field] = append(res[field], value)
		}
	}
	return
}

func (umongo *MongoDbUtil) FindWrapError(filter bson.M,
	request interface{}, pointerDecodeTo interface{},
) (paginationResp *PaginationResponse, err error) {
	if filter == nil {
		filter = bson.M{}
	}
	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	var requestPagination Request_Pagination
	//* ----------------------------- SET FILTER REQUEST ---------------------------- */
	switch requestAsType := request.(type) {
	case Request:
		filter = requestAsType.Handle(filter)
		requestPagination = requestAsType.Request_Pagination
	case Request_Pagination:
		requestPagination = requestAsType
	default:
		log.Printf("unknow type: %T\n", requestAsType)
	}

	//* ------------------------- SET PAGINATION OPTIONS ------------------------- */
	//? Sorting
	findOptions := options.FindOptions{}
	order := -1
	if strings.ToLower(requestPagination.Order) == "asc" {
		order = 1
	}
	if orderBy := requestPagination.OrderBy; orderBy != "" {
		findOptions.Sort = bson.M{orderBy: order}
	}
	if len(requestPagination.OverwriteSort) > 0 {
		findOptions.SetSort(requestPagination.OverwriteSort)
	}

	//? Skip + Limit
	skip := requestPagination.Page
	if skip > 0 {
		skip--
	}
	if requestPagination.Size == 0 {
		requestPagination.Size = 3
	}
	skip *= requestPagination.Size
	findOptions.Skip = &skip
	findOptions.Limit = &requestPagination.Size

	if err = umongo.BaseFind(filter, findOptions, pointerDecodeTo); err != nil {
		log.Println(err)
		return
	}

	//* ------------------------- SET RESPONSE PAGINATION ------------------------ */
	totalElements, err := col.CountDocuments(umongo.Ctx, filter)
	if err != nil {
		log.Println(err)
		return
	}
	paginationResp = &PaginationResponse{
		Size:          int(requestPagination.Size),
		TotalElements: totalElements,
		TotalPages:    int64(math.Ceil(float64(totalElements) / float64(requestPagination.Size))),
	}

	if totalElements == 0 {
		asJson, _ := json.Marshal(filter)
		err = errors.New("no data found")
		log.Println(err, umongo.CollectionName)
		fmt.Println(string(asJson))
	}
	return
}

func GetErrForResponse(err error) (res string) {
	if err != nil {
		res = "FAIL"
	} else {
		res = "OK"
	}
	return
}

func (umongo *MongoDbUtil) Find(filter bson.M,
	request interface{}, pointerDecodeTo interface{},
) (paginationResp *PaginationResponse, errMessage string) {
	paginationResp, err := umongo.FindWrapError(filter, request, pointerDecodeTo)
	errMessage = GetErrForResponse(err)
	return
}

func (umongo *MongoDbUtil) CheckDuplicate(id string, listFilterOr []bson.M) (err error) {
	var checkDuplicate bson.M
	if err := umongo.BaseFindOne(bson.M{"$or": listFilterOr}, &checkDuplicate); err != nil {
		log.Println(err)
		return nil
	}

	var oldData bson.M
	if id != "" {
		if err := umongo.FindOne("_id", id, &oldData); err != nil {
			log.Println(err)
			return err
		}
	}

	for _, filterOr := range listFilterOr {
		for field, newDataValue := range filterOr {
			if newDataValue == checkDuplicate[field] {
				//* To allow update, check if the old data == new data
				//? but the new data not duplicate in another data
				if oldData[field] != newDataValue {
					switch field {
					case "username":
						return errors.New("username already used, please use another username")
					}
					return errors.New(field + " is already exists, and need to be unique")
				}
			}
		}
	}

	return
}

const statusArchive = "archive"

func (umongo *MongoDbUtil) getSoftDeleteSetUpdate() bson.M {
	return bson.M{"$set": bson.M{"status": statusArchive}}
}

func (umongo *MongoDbUtil) DeleteOne(key, value string) (err error) {
	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	filter := bson.M{key: value}
	res, err := col.UpdateOne(umongo.Ctx, filter, umongo.getSoftDeleteSetUpdate())
	if err != nil {
		log.Println(err)
		return
	}

	if res.MatchedCount == 0 {
		err = errors.New("data not found")
		fmt.Printf("[%s] filter: %v\n", umongo.CollectionName, filter)
	}
	return
}

func (umongo *MongoDbUtil) Delete(filter bson.M) (errMessage string) {
	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	col := client.Database(umongo.DbName).Collection(umongo.CollectionName, umongo.defaultCollectionOption())

	res, err := col.UpdateMany(umongo.Ctx, filter, umongo.getSoftDeleteSetUpdate())
	if err != nil {
		log.Println(err)
		return
	}
	if res.ModifiedCount == 0 {
		errMessage = "Data not found"
		fmt.Printf("[%s] filter: %v\n", umongo.CollectionName, filter)
	}

	return
}

func (umongo *MongoDbUtil) CreateViewIfNotExists(viewName string, pipeline []bson.M) (err error) {
	client, err := umongo.connect()
	if err != nil {
		return
	}
	defer umongo.Disconnect(client)
	db := client.Database(umongo.DbName)
	if listCollectionName, err := db.ListCollectionNames(umongo.Ctx, bson.M{}, &options.ListCollectionsOptions{}); err != nil {
		log.Println(err)
		return err
	} else {
		if slices.Contains(listCollectionName, viewName) {
			return nil
		}
	}

	if err := db.CreateView(umongo.Ctx, viewName, umongo.CollectionName, pipeline, &options.CreateViewOptions{}); err != nil {
		log.Println(viewName, err)
		return err
	}

	log.Printf("%s->%s created\n", viewName, umongo.CollectionName)
	return
}
