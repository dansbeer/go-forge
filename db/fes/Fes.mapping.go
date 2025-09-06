package fes

import (
	"fmt"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/tidwall/gjson"
)

const (
	REMOVE_MARK = "{{{REMOVE}}}"
)

func cleanKey(key string) string {
	return strings.NewReplacer(
		REMOVE_MARK+" - ", "",
	).Replace(key)
}

func getKeyAsString(key any) (res string) {
	switch asType := key.(type) {
	case string:
		res = asType
	case float64:
		// res = fmt.Sprintf("%.0f", asType)
		res = REMOVE_MARK
	}
	return
}

type Opt struct {
	GenerateFlat        bool
	DirectGetDocCount   bool
	GenerateMultiSeries bool
	GenerateStacked     bool
}

func baseEsMapping_Loop(key any, bucket gjson.Result, mappingType *string, opt Opt) (res []map[string]any) {
	newDatum := map[string]any{}
	newDatum["key"] = key
	newDatum["value"] = bucket.Get("doc_count").Value()

	if unique := bucket.Get("parent_count.unique.value"); unique.Exists() { //? For nested terms
		newDatum["value"] = unique.Value()
	}

	//? replace if metrix exists
	if metrixValue := bucket.Get("*.value"); metrixValue.Exists() {
		newDatum["value"] = metrixValue.Value()
	}
	if metrixValue := bucket.Get("Distinct_Count.value"); metrixValue.Exists() { //? Priority agg with name value_count as the value
		newDatum["value"] = metrixValue.Value()
	}

	//? Handle another subs aggs
	if opt.DirectGetDocCount {
		opt.GenerateFlat = true
	}
	if opt.GenerateFlat {
		opt.GenerateMultiSeries = true
	}

	if opt.GenerateMultiSeries {
		bucket.ForEach(func(subKey, value gjson.Result) bool {
			switch subKey.String() {
			case "key", "doc_count", "key_as_string":
				return true
			}
			flatKey := cleanKey(getKeyAsString(key) + " - " + subKey.Str)
			if tryGetValue := value.Get("value"); tryGetValue.Exists() {
				newDatum[subKey.Str] = value.Get("value").Value()
			} else {
				newDatum[subKey.Str] = value.Value()
				if docCount := value.Get("doc_count"); docCount.Exists() && opt.DirectGetDocCount {
					newDatum[subKey.Str] = docCount.Value()
				}
			}
			if opt.GenerateFlat && !opt.DirectGetDocCount {
				newDatum[flatKey] = newDatum[subKey.Str]
			}
			*mappingType = "multiSeries"
			return true
		})
	}

	subData, subMappingType := baseEsMapping_KeyValueData(bucket, opt)
	if len(subData) > 0 {
		newDatum["data"] = subData
		if opt.GenerateFlat {
			for _, datum := range subData {
				for subKey, subValue := range datum {
					switch subKey {
					case "key", "value":
						continue
					}

					keyOfSubValue := cleanKey(strings.Join([]string{getKeyAsString(key), subKey}, " - "))
					if !strings.Contains(keyOfSubValue, " - ") {
						continue
					}
					newDatum[keyOfSubValue] = subValue
				}
			}
		}
		if opt.DirectGetDocCount {
			delete(newDatum, "data")
		}
		if opt.GenerateStacked {
			for _, datum := range subData {
				subKey := fmt.Sprint(datum["key"])
				subKey = strings.ToLower(strings.ReplaceAll(subKey, " ", "_"))
				newDatum[subKey] = datum["value"]
			}
			delete(newDatum, "data")
			delete(newDatum, "value")
		}
	}
	if subMappingType != "" {
		*mappingType = subMappingType
	}
	res = append(res, newDatum)
	return
}

func baseEsMapping_KeyValueData(buckets gjson.Result, opt Opt) (res []map[string]any, mappingType string) {
	tryBuckets := buckets.Get("*.buckets")
	if !tryBuckets.Exists() {
		tryBuckets = buckets.Get("*.*.buckets") //? Happens when using nested aggs
		if !tryBuckets.Exists() {               //? happens when using multiple nested aggs, e.g. aggs: nested>nested>terms
			tryBuckets = buckets.Get("*.*.*.buckets")
		}
	}

	//? handle: direct to metric (without any bucket)
	if !tryBuckets.Exists() && !buckets.Get("*.*.hits").Exists() &&
		buckets.Get("*.doc_count").Exists() {
		tryBuckets = buckets
	}
	if tryBuckets.IsObject() {
		tryBuckets.ForEach(func(key, value gjson.Result) bool {
			res = append(res, baseEsMapping_Loop(key.Str, value, &mappingType, opt)...)
			return true
		})
	} else {
		for _, bucket := range tryBuckets.Array() {
			res = append(res, baseEsMapping_Loop(bucket.Get("key").Value(), bucket, &mappingType, opt)...)
		}
	}
	if tryBuckets.Exists() {
		return
	}

	reverseNested := buckets.Get("rn")
	if reverseNested.Exists() { //? Handle reverseNested
		buckets = reverseNested
	}
	if tryHits := buckets.Get("*.hits.hits.#._source"); tryHits.Exists() { //? Handle Top hits
		for _, each := range tryHits.Array() {
			source, _ := each.Value().(map[string]any)
			source["rn"] = reverseNested.Get("doc_count").Value()
			res = append(res, map[string]any{"source": source})
		}
	}

	metricOnly := buckets.Get("metricOnly.value")
	if metricOnly.Exists() {
		res = append(res, map[string]any{"value": metricOnly.Value()}) //? Capture metric only
	} else {
		if tryGetMetric := buckets.Get("*.value"); !tryBuckets.Exists() && tryGetMetric.Exists() {
			buckets.ForEach(func(key, value gjson.Result) bool {
				res = append(res, map[string]any{
					"key":   key.Str,
					"value": value.Get("value").Value(),
				}) //? Capture metric only
				return true
			})
		}
	}
	return
}

func EsMapping_KeyValueDataAndGetMappingTypeWithOpt(respEs gjson.Result, opt Opt) (res []map[string]any, mappingType string) {
	res, mappingType = baseEsMapping_KeyValueData(respEs.Get("aggregations"), opt)
	// res = FillEmptyKey(res, -0)
	return
}

func EsMapping_KeyValueDataAndGetMappingType(respEs gjson.Result) (res []map[string]any, mappingType string) {
	res, mappingType = EsMapping_KeyValueDataAndGetMappingTypeWithOpt(respEs, Opt{})
	return
}

func EsMapping_KeyValueData(respEs gjson.Result) (res []map[string]any) {
	res, _ = EsMapping_KeyValueDataAndGetMappingType(respEs)
	return
}

func FillEmptyKey(raw []map[string]any, nestedIndex int) (res []map[string]any) {
	res = raw
	fmt.Printf("nestedIndex: %v\n", aurora.Yellow(nestedIndex))
	for _, each := range res {
		asListMap, exists := each["data"].([]map[string]any)
		if !exists {
			return
		}
		FillEmptyKey(asListMap, nestedIndex+1)
	}
	return
}
