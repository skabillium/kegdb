package keg

type KeyMetadata struct {
	Header Header
	offset int
}

type KeyDir map[string]KeyMetadata
