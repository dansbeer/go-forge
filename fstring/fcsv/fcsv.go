package fcsv

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type CSVReader struct {
	byFilePath string
	byString   string
}

func NewCSVReader(filePath string) *CSVReader {
	return &CSVReader{byFilePath: filePath}
}

func NewCSVReader_ByString(byString string) *CSVReader {
	return &CSVReader{byString: byString}
}

func (r *CSVReader) Read() ([]map[string]string, error) {
	start := time.Now()
	var in io.Reader
	if r.byFilePath != "" {
		file, err := os.Open(r.byFilePath)
		if err != nil {
			return nil, err
		}
		in = file
		defer file.Close()
	} else if r.byString != "" {
		in = strings.NewReader(r.byString)
	}

	reader := csv.NewReader(in)
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	header := records[0]
	data := make([]map[string]string, len(records)-1)

	for i, record := range records[1:] {
		row := make(map[string]string)
		for j, value := range record {
			row[header[j]] = value
		}
		data[i] = row
	}

	log.Printf("Waktu membaca file: %s\n", time.Since(start))
	return data, nil
}
