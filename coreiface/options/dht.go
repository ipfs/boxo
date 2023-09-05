package options

// Deprecated: use [RoutingProvideSettings] instead
type DhtProvideSettings = RoutingProvideSettings

// Deprecated: use [RoutingFindProvidersSettings] instead
type DhtFindProvidersSettings = RoutingFindProvidersSettings

// Deprecated: use [RoutingProvideOption] instead
type DhtProvideOption = RoutingProvideOption

// Deprecated: use [RoutingFindProvidersOption] instead
type DhtFindProvidersOption = RoutingFindProvidersOption

// Deprecated: use [RoutingProvideOptions] instead
func DhtProvideOptions(opts ...DhtProvideOption) (*DhtProvideSettings, error) {
	return RoutingProvideOptions(opts...)
}

// Deprecated: use [RoutingFindProvidersOptions] instead
func DhtFindProvidersOptions(opts ...DhtFindProvidersOption) (*DhtFindProvidersSettings, error) {
	return RoutingFindProvidersOptions(opts...)
}

// Deprecated: use [Routing] instead
var Dht routingOpts
