package fes

import (
	"log"
	"sort"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/exp/slices"
)

func UpdateRespArrayOrder(esResp, targetPath string, listOrderBy []string, order [][]string) (res string) {
	res = esResp
	targetArray := gjson.Get(esResp, targetPath).Array()
	handleSorting := func(target []gjson.Result, path string) {
		asListMap := []map[string]any{}
		if len(listOrderBy) > 1 {
			split := map[string][]gjson.Result{}
			for _, each := range target {
				key := each.Get(listOrderBy[0]).String()
				split[key] = append(split[key], each)
			}

			for _, each := range split {
				sort.Slice(each, func(i, j int) bool {
					return slices.Index(order[1], each[i].Get(listOrderBy[1]).String()) < slices.Index(order[1], each[j].Get(listOrderBy[1]).String())
				})
			}

			for _, each := range order[0] {
				for _, each := range split[each] {
					asMap, _ := each.Value().(map[string]any)
					asListMap = append(asListMap, asMap)
				}
			}
		} else if len(listOrderBy) == 1 {
			sort.Slice(target, func(i, j int) bool {
				return slices.Index(order[0], target[i].Get(listOrderBy[0]).String()) < slices.Index(order[0], target[j].Get(listOrderBy[0]).String())
			})
			for _, each := range target {
				asMap, _ := each.Value().(map[string]any)
				asListMap = append(asListMap, asMap)
			}
		}
		res, _ = sjson.Set(res, path, asListMap)
	}

	if strings.Contains(targetPath, "#") {
		for _, each := range targetArray {
			listBucket := each.Array()
			handleSorting(listBucket, each.Path(res))
		}
	} else {
		handleSorting(targetArray[1:], targetPath)
	}
	log.Println("\n", res)
	return
}
