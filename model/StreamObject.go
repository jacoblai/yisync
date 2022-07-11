package model

type StreamObject struct {
	Id                *WatchId `bson:"_id"`
	OperationType     string
	FullDocument      map[string]interface{}
	Ns                NS
	UpdateDescription map[string]interface{}
	DocumentKey       map[string]interface{}
}

type NS struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type WatchId struct {
	Data string `bson:"_data"`
}
