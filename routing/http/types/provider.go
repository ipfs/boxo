package types

// WriteProviderRecord is a type that enforces structs to imlement it to avoid confusion
type WriteProviderRecord interface {
	IsWriteProviderRecord()
}

// ReadProviderRecord is a type that enforces structs to imlement it to avoid confusion
type ReadProviderRecord interface {
	IsReadProviderRecord()
}

// ProviderResponse is implemented for any ProviderResponse. It needs to have a Protocol field.
type ProviderResponse interface {
	GetProtocol() string
	GetSchema() string
}
