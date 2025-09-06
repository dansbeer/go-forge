package fes

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/exp/slices"
)

func baseEs_SearchPathAggs(queryFull []byte, target gjson.Result, res *[]string) {
	for aggName, each := range target.Map() {
		if slices.Contains([]string{"terms", "significant_terms", "date_histogram"}, aggName) {
			path, _ := strings.CutSuffix(each.Path(string(queryFull)), "."+aggName)
			*res = append(*res, path)
			subAggs := each.Get("aggs.*")
			if !subAggs.Exists() {
				subAggs = target.Get("aggs.*")
			}
			baseEs_SearchPathAggs(queryFull, subAggs, res)
		}
	}
}

func Es_SearchPathAggs(queryFull []byte) (res []string) {
	baseEs_SearchPathAggs(queryFull, gjson.GetBytes(queryFull, "aggs.*"), &res)
	return
}

func EsMoveQuery(query, toPath, fromPath string) (res string) {
	var err error
	res = query

	res, err = sjson.Set(res, toPath, gjson.Get(res, fromPath).Value())
	if err != nil {
		log.Println(err)
		return
	}
	res, err = sjson.Delete(res, fromPath)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

func EsMergeNestedQueryWithSamePath(esQuery string) (result string) {
	for _, pathQuery := range []string{"filter", "must", "must_not"} {
		listQueryByNestedPath, oldNestedPathGjson := map[string][]any{}, []string{}

		gjson.Get(esQuery, "query.bool."+pathQuery).ForEach(func(key, value gjson.Result) bool {
			if nestedPath := value.Get("nested.path"); nestedPath.Exists() {
				oldNestedPathGjson = append(oldNestedPathGjson, value.Path(esQuery))
				nestedQuery := value.Get("nested.query")
				if query := nestedQuery; query.IsObject() {
					listQueryByNestedPath[nestedPath.Str] = append(listQueryByNestedPath[nestedPath.Str], query.Value())
				} else if query := nestedQuery; query.IsArray() {
					for _, each := range query.Array() {
						listQueryByNestedPath[nestedPath.Str] = append(listQueryByNestedPath[nestedPath.Str], each.Value())
					}
				}
			}
			return true
		})

		for _, nestedPath := range oldNestedPathGjson {
			escapedNestedPath := []string{}
			for _, split := range strings.Split(nestedPath, ".") {
				if isNumber, errParseFloat := strconv.ParseFloat(split, 64); errParseFloat == nil {
					split = fmt.Sprintf(":%d", int64(isNumber))
				}
				escapedNestedPath = append(escapedNestedPath, split)
			}
			if len(escapedNestedPath) > 0 {
				nestedPath = strings.Join(escapedNestedPath, ".")
			}
			esQuery, _ = sjson.Delete(esQuery, nestedPath)
		}

		for nestedPath, nestedQuery := range listQueryByNestedPath {
			esQuery, _ = sjson.Set(esQuery, fmt.Sprintf("query.bool.%s.-1", pathQuery), map[string]any{
				"nested": map[string]any{
					"path": nestedPath, "query": map[string]any{"bool": map[string]any{"must": nestedQuery}}, "inner_hits": map[string]any{},
				},
			})
		}
	}
	return esQuery
}
