package fid

import (
	"strings"

	"github.com/google/uuid"
)

func GenerateID() (res string) {
	res = strings.ReplaceAll(uuid.New().String(), "-", "")
	return
}
