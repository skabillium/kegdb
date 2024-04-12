package keg

type KeyMetadata struct {
	Header Header
	offset int
	fileId int
}

type KeyDir map[string]KeyMetadata
