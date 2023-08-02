package types

// ProviderResponse is implemented for any ProviderResponse. It needs to have a Protocol field.
type ProviderResponse interface {
	GetSchema() string
}
