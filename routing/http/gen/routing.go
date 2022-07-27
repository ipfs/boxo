//go:generate go run ./routing.go
package main

import (
	"os"
	"path"

	log "github.com/ipfs/go-log/v2"
	"github.com/ipld/edelweiss/compile"
	"github.com/ipld/edelweiss/defs"
)

var proto = defs.Defs{

	// delegated routing service definition
	defs.Named{
		Name: "DelegatedRouting",
		Type: defs.Service{
			Methods: defs.Methods{
				defs.Method{
					Name: "FindProviders",
					Type: defs.Fn{
						Arg:    defs.Ref{Name: "FindProvidersRequest"},
						Return: defs.Ref{Name: "FindProvidersResponse"},
					},
				},
				defs.Method{
					Name: "GetIPNS",
					Type: defs.Fn{
						Arg:    defs.Ref{Name: "GetIPNSRequest"},
						Return: defs.Ref{Name: "GetIPNSResponse"},
					},
				},
				defs.Method{
					Name: "PutIPNS",
					Type: defs.Fn{
						Arg:    defs.Ref{Name: "PutIPNSRequest"},
						Return: defs.Ref{Name: "PutIPNSResponse"},
					},
				},
			},
		},
	},

	// FindProviders request type
	defs.Named{
		Name: "FindProvidersRequest",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{
					Name:   "Key",
					GoName: "Key",
					Type:   defs.Ref{Name: "LinkToAny"},
				},
			},
		},
	},

	// FindProviders response type
	defs.Named{
		Name: "FindProvidersResponse",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{
					Name:   "Providers",
					GoName: "Providers",
					Type: defs.Named{
						Name: "ProvidersList",
						Type: defs.List{Element: defs.Ref{Name: "Provider"}},
					},
				},
			},
		},
	},

	// GetIPNS request type
	defs.Named{
		Name: "GetIPNSRequest",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{Name: "ID", GoName: "ID", Type: defs.Bytes{}},
			},
		},
	},

	// GetIPNS response type
	defs.Named{
		Name: "GetIPNSResponse",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{Name: "Record", GoName: "Record", Type: defs.Bytes{}},
			},
		},
	},

	// PutIPNS request type
	defs.Named{
		Name: "PutIPNSRequest",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{Name: "ID", GoName: "ID", Type: defs.Bytes{}},
				defs.Field{Name: "Record", GoName: "Record", Type: defs.Bytes{}},
			},
		},
	},

	// PutIPNS response type
	defs.Named{
		Name: "PutIPNSResponse",
		Type: defs.Structure{},
	},

	// general routing types
	defs.Named{
		Name: "LinkToAny",
		Type: defs.Link{To: defs.Any{}},
	},

	defs.Named{
		Name: "Provider",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{
					Name:   "Node",
					GoName: "ProviderNode",
					Type:   defs.Ref{Name: "Node"},
				},
				defs.Field{
					Name:   "Proto",
					GoName: "ProviderProto",
					Type:   defs.Ref{Name: "TransferProtocolList"},
				},
			},
		},
	},

	defs.Named{
		Name: "TransferProtocolList",
		Type: defs.List{Element: defs.Ref{Name: "TransferProtocol"}},
	},

	defs.Named{
		Name: "Node",
		Type: defs.Inductive{
			Cases: defs.Cases{
				defs.Case{Name: "peer", GoName: "Peer", Type: defs.Ref{Name: "Peer"}},
			},
			Default: defs.DefaultCase{
				GoKeyName:   "DefaultKey",
				GoValueName: "DefaultValue",
				Type:        defs.Any{},
			},
		},
	},

	defs.Named{
		Name: "Peer",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{Name: "ID", GoName: "ID", Type: defs.Bytes{}},
				defs.Field{Name: "Multiaddresses", GoName: "Multiaddresses", Type: defs.List{Element: defs.Bytes{}}},
			},
		},
	},

	defs.Named{
		Name: "TransferProtocol",
		Type: defs.Inductive{
			Cases: defs.Cases{
				defs.Case{Name: "2304", GoName: "Bitswap", Type: defs.Ref{Name: "BitswapProtocol"}},
				defs.Case{Name: "2320", GoName: "GraphSyncFILv1", Type: defs.Ref{Name: "GraphSyncFILv1Protocol"}},
			},
			Default: defs.DefaultCase{
				GoKeyName:   "DefaultKey",
				GoValueName: "DefaultValue",
				Type:        defs.Any{},
			},
		},
	},

	defs.Named{
		Name: "BitswapProtocol",
		Type: defs.Structure{},
	},

	defs.Named{
		Name: "GraphSyncFILv1Protocol",
		Type: defs.Structure{
			Fields: defs.Fields{
				defs.Field{Name: "PieceCID", GoName: "PieceCID", Type: defs.Ref{Name: "LinkToAny"}},
				defs.Field{Name: "VerifiedDeal", GoName: "VerifiedDeal", Type: defs.Bool{}},
				defs.Field{Name: "FastRetrieval", GoName: "FastRetrieval", Type: defs.Bool{}},
			},
		},
	},
}

var logger = log.Logger("proto generator")

func main() {
	wd, err := os.Getwd()
	if err != nil {
		logger.Errorf("working dir (%v)\n", err)
		os.Exit(-1)
	}
	dir := path.Join(wd, "proto")
	x := &compile.GoPkgCodegen{
		GoPkgDirPath: dir,
		GoPkgPath:    "github.com/ipfs/go-delegated-routing/gen/proto",
		Defs:         proto,
	}
	goFile, err := x.Compile()
	if err != nil {
		logger.Errorf("compilation (%v)\n", err)
		os.Exit(-1)
	}
	if err = os.Mkdir(dir, 0755); err != nil {
		logger.Errorf("making pkg dir (%v)\n", err)
		os.Exit(-1)
	}
	if err = goFile.Build(); err != nil {
		logger.Errorf("build (%v)\n", err)
		os.Exit(-1)
	}
}
