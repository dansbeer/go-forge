package fstring

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/vigneshuvi/GoDateFormat"
)

const (
	DATE_UNIX_MILLI = "unix_milli"
)

var Id_Month = map[string]string{
	"January":   "Januari",
	"February":  "Februari",
	"March":     "Maret",
	"April":     "April",
	"May":       "Mei",
	"June":      "Juni",
	"July":      "Juli",
	"August":    "Agustus",
	"September": "September",
	"October":   "Oktober",
	"November":  "November",
	"December":  "Desember",
}

var minusPlusReplacer = strings.NewReplacer("-", "", "+", "")

func ConvertDateString(raw, targetFormat string) (res any) {
	for en, id := range Id_Month {
		raw = strings.ReplaceAll(raw, id, en)
	}

	parsedTime, err := dateparse.ParseAny(raw)
	if err != nil && strings.Contains(raw, "now") && strings.Contains(raw, "/d") {
		now := time.Now()
		parsedTime = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.UTC().Location())
		listFind := regexp.MustCompile(`([-+]\d+[dh])`).FindAllStringSubmatch(raw, -1)
		for _, find := range listFind {
			capture := find[1]
			if cut, found := strings.CutSuffix(capture, "d"); found { //? Handle -1d
				digitDay, _ := strconv.ParseInt(minusPlusReplacer.Replace(cut), 10, 32)
				capture = strings.Replace(capture, minusPlusReplacer.Replace(capture), fmt.Sprintf("%dh", digitDay*24), 1)
			}

			minusOrPlusTime, errParseDuration := time.ParseDuration(capture)
			if errParseDuration != nil {
				log.Println(errParseDuration)
				return
			}

			parsedTime = parsedTime.Add(minusOrPlusTime)
		}
	} else if err != nil {
		if raw == "now" {
			parsedTime = time.Now()
		} else {
			return
		}
	} else if len(regexp.MustCompile("[A-z]").FindAllString(raw, -1)) > 0 {
		log.Println("Regex detect a - z")
		return
	}
	switch targetFormat {
	case DATE_UNIX_MILLI:
		res = parsedTime.UnixMilli()
	default:
		res = parsedTime.Format(GoDateFormat.ConvertFormat(targetFormat))
	}
	return
}

func DetectDateFromString(in string) (isDate bool) {
	//? not nil = date
	return ConvertDateString(in, DATE_UNIX_MILLI) != nil
}
