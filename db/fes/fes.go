package fes

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/dansbeer/go-forge/db/fmongo"
	"github.com/dansbeer/go-forge/enum"
	"github.com/dansbeer/go-forge/fstring"
	"github.com/olivere/elastic/v7"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Ues struct {
	ctx          context.Context
	client       *elastic.Client
	indexName    string
	indexMapping string
	ForceNewId   bool
}

func GetIndexDateFormat_yyyyMM() string {
	return "200601"
}

func GetIndexDateFormat_yyyyMMdd() string {
	return "20060102"
}

func GetIndexNameWithDate_ByFormat(indexName, format string) (res string) {
	return fmt.Sprintf("%s-%s", indexName, time.Now().Format(format))
}

func GetIndexNameWithDate(indexName string) (res string) {
	return GetIndexNameWithDate_ByFormat(indexName, GetIndexDateFormat_yyyyMM())
}

// ? This version can handle custom dateFormat
func BaseGetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping string, options ...elastic.ClientOptionFunc) (res *Ues) {
	options = append(options, elastic.SetURL(esUrl))
	client, err := elastic.NewClient(options...)
	if err != nil {
		log.Println(err)
		return
	}

	indexName = GetIndexNameWithDate_ByFormat(indexName, dateFormat)
	res = &Ues{ctx: context.Background(), client: client, indexName: indexName, indexMapping: indexMapping}
	res.CreateIndexIfNotExists()
	return
}

func GetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping string) (res *Ues) {
	return BaseGetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping)
}

func BaseLog_GetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping string, options ...elastic.ClientOptionFunc) (res *Ues) {
	res = BaseGetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping, options...)
	if res == nil {
		return nil
	}
	res.ForceNewId = true
	return
}

func Log_GetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping string) (res *Ues) {
	return BaseLog_GetClientSplitByDate_ByFormat(dateFormat, esUrl, indexName, indexMapping)
}

func GetClient(splitIndexByDate bool, esUrl, indexName, indexMapping string) (res *Ues) {
	client, err := elastic.NewClient(elastic.SetURL(esUrl))
	if err != nil {
		log.Println(err)
		return
	}

	if splitIndexByDate {
		indexName = GetIndexNameWithDate(indexName)
	}
	res = &Ues{ctx: context.Background(), client: client, indexName: indexName, indexMapping: indexMapping}
	res.CreateIndexIfNotExists()
	return
}

func (ues *Ues) CreateIndexIfNotExists() {
	defer ues.client.Stop()

	exists, err := ues.client.IndexExists(ues.indexName).Do(ues.ctx)
	if err != nil {
		log.Println(err)
		return
	}
	if exists {
		return
	}

	mapping := fmt.Sprintf(`{
		"settings": {
			"number_of_shards": 2,
			"number_of_replicas": 1
		},
		"mappings": %s
	}`, ues.indexMapping)
	mapping, _ = sjson.Set(mapping, "mappings.properties.id", map[string]any{"type": "keyword"})
	mapping, _ = sjson.Set(mapping, "mappings.properties.status", map[string]any{"type": "keyword"})
	mapping, _ = sjson.Set(mapping, "mappings.properties.created_at", map[string]any{"type": "date"})
	mapping, _ = sjson.Set(mapping, "mappings.properties.updated_at", map[string]any{"type": "date"})

	createIndex, err := ues.client.CreateIndex(ues.indexName).BodyString(mapping).Do(ues.ctx)
	if err != nil {
		log.Println(err)
		return
	}
	if !createIndex.Acknowledged {
		log.Println("!ack")
		return
	}
}

func (ues *Ues) UpsertDoc(newDoc any) (resId string) {
	defer ues.client.Stop()

	asStr, _ := json.Marshal(newDoc)
	id := gjson.GetBytes(asStr, "id").String()
	if id == "" {
		id = fstring.GenerateID()
		if ues.ForceNewId {
			id += fmt.Sprintf("%s%x", id, md5.Sum([]byte(id)))
		}
		asStr, _ = sjson.SetBytes(asStr, "id", id)
		asStr, _ = sjson.SetBytes(asStr, "created_at", time.Now().UnixMilli())
	}
	asStr, _ = sjson.SetBytes(asStr, "updated_at", time.Now().UnixMilli())

	res, err := ues.client.Index().Index(ues.indexName).Id(id).BodyString(string(asStr)).Do(ues.ctx)
	if err != nil {
		log.Println(err)
		return
	}

	fmt.Printf("Indexed %s to index %s\n", res.Id, res.Index)
	return res.Id
}

func (ues *Ues) GetOneDoc(id string, ptrDecodeTo any) (errMessage string) {
	defer ues.client.Stop()

	getRes, err := ues.client.Get().
		Index(ues.indexName).
		Id(id).
		Do(ues.ctx)
	if err != nil {
		log.Println(err)
		return
	}
	if !getRes.Found {
		errMessage = "Doc not found: " + id
		fmt.Println(errMessage)
		return
	}

	if err := json.Unmarshal(getRes.Source, ptrDecodeTo); err != nil {
		log.Println(err)
		return
	}
	return
}

func (ues *Ues) GetDoc(ptrDecodeTo any, request fmongo.Request, filter *elastic.BoolQuery) (paginationResp *fmongo.PaginationResponse, err error) {
	defer ues.client.Stop()

	request.Page = (request.Page - 1) * request.Size

	if filter == nil {
		filter = elastic.NewBoolQuery()
	}
	filter.MustNot(elastic.NewTermQuery("status", "archive"))

	search := elastic.NewSearchSource().
		TrackTotalHits(true).
		Query(filter).
		From(int(request.Page)).Size(int(request.Size))
	if request.Order != "" && request.OrderBy != "" {
		isAsc := false
		if request.Order == "ASC" {
			isAsc = true
		}
		search = search.Sort(request.OrderBy, isAsc)
	}
	searchRes, err := ues.client.Search().Index(ues.indexName).SearchSource(search).Do(ues.ctx)
	if err != nil {
		log.Println(err)
		return
	}
	query, _ := search.Source()
	asJson, _ := json.Marshal(query)
	fmt.Println(string(asJson))

	//* -------------------------------- DATA RESP ------------------------------- */
	data := []map[string]any{}
	for _, hit := range searchRes.Hits.Hits {
		datum := map[string]any{}
		if err = json.Unmarshal(hit.Source, &datum); err != nil {
			log.Println(err)
			continue
		}
		if _, found := datum["id"]; !found {
			datum["id"] = hit.Id
		}
		data = append(data, datum)
	}
	dataJson, _ := json.Marshal(data)
	if err = json.Unmarshal(dataJson, &ptrDecodeTo); err != nil {
		log.Println(err)
		return
	}
	//* ----------------------------- PAGINATION RESP ---------------------------- */
	totalElements := searchRes.Hits.TotalHits.Value
	if totalElements == 0 {
		err = errors.New(enum.Error_NoDataFound.String())
	}
	paginationResp = &fmongo.PaginationResponse{
		Size:          int(request.Size),
		TotalElements: totalElements,
		TotalPages:    int64(math.Ceil(float64(totalElements) / float64(request.Size))),
	}
	return
}

func (ues *Ues) DeleteDoc(id string) (message string) {
	defer ues.client.Stop()

	existsDoc := map[string]any{}
	_ = ues.GetOneDoc(id, &existsDoc)
	existsDoc["status"] = "archive"

	_ = ues.UpsertDoc(existsDoc)
	return "OK"
}
