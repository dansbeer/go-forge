package fmongo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type Range struct {
	Field string `json:"field"`
	Start int64  `json:"start,omitempty" example:"1646792565000"`
	End   int64  `json:"end,omitempty" example:"1646792565000"`
}
type Request_Search struct {
	Range *Range `json:"range,omitempty" swaggerignore:"true"`
}

type Request struct {
	Request_Pagination
	Request_Search
}

func (o Request_Search) Handle(filter bson.M) (res bson.M) {
	res = filter

	if o.Range != nil {
		if o.Range.Start == 0 && o.Range.End == 0 {
			timeNow := time.Now()
			o.Range.End = timeNow.UnixMilli()
			o.Range.Start = timeNow.AddDate(0, 0, -7).UnixMilli()
		}
		field := "created_at"
		if o.Range.Field != "" {
			field = o.Range.Field
		}
		filter[field] = bson.M{
			"$gte": o.Range.Start, "$lt": o.Range.End,
		}
	}

	return
}

type PaginationResponse struct {
	Size          int   `json:"size,omitempty"`
	TotalPages    int64 `json:"totalPages,omitempty"`
	TotalElements int64 `json:"totalElements,omitempty"`
}

type Request_Pagination struct {
	OrderBy string `json:"orderBy" example:"createdAt" form:"orderBy"`
	Order   string `json:"order" example:"DESC" form:"order"`
	//? Or use this instead for multiple sorting
	OverwriteSort bson.D `json:"overwriteSort"`

	Page int64 `example:"1" json:"page" form:"page"`
	Size int64 `example:"11" json:"size" form:"size"`
}
