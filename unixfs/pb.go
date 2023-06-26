// @Jorropo: The reason why I picked the solution to write a snowflake protobuf
// decoder here is because I couldn't find a zero allocation protobuf decoder generator.
// I do not count pooling or arenas as zero allocation btw.
// If you are reading this text trying to add more fields and this is too painfull
// to deal with feel free to remove this code and replace it with an allocation
// codegen decoder. Ping me too if I'm still around I might revert your changes
// and bring back the allocation free decoder but with the new feature.
package unixfs

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	_ = iota
	pbDirectory
	pbFile
	pbMetadata
	pbSymlink
	pbHAMTShard
)

// Reference:
//
//  message Data {
//  	enum DataType {
//  		Raw = 0;
//  		Directory = 1;
//  		File = 2;
//  		Metadata = 3;
//  		Symlink = 4;
//  		HAMTShard = 5;
//  	}
//
//  	required DataType Type = 1;
//  	optional bytes Data = 2;
//  	optional uint64 filesize = 3;
//  	repeated uint64 blocksizes = 4;
//
//  	optional uint64 hashType = 5;
//  	optional uint64 fanout = 6;
//  }
//
//  message Metadata {
//  	optional string MimeType = 1;
//  }
//
// 	message PBLink {
// 	  // binary CID (with no multibase prefix) of the target object
// 	  optional bytes Hash = 1;
//
// 	  // UTF-8 string name
// 	  optional string Name = 2;
//
// 	  // cumulative size of target object
// 	  optional uint64 Tsize = 3;
// 	}
//
// 	message PBNode {
// 	  // refs to other objects
// 	  repeated PBLink Links = 2;
//
// 	  // Unixfs message inside the user opaque data
// 	  optional Data Data = 1;
// 	}

func parsePB[Self, Children cid.Storage](
	fileChildrens []FileEntry[Children],
	directoryChildrens []DirectoryEntry[Children],
	inCid cid.GenericCid[Self], origData []byte,
) (dataType uint64, _ []FileEntry[Children], fileLinks, blocksizes uint, _ []DirectoryEntry[Children], content []byte, selfTSize uint64, _ error) {
	selfTSize = 1
	data := origData

	moveZeroNamedDirectoryEntriesToDirectoryChildrens := func(extra int) {
		// some zero named children were confused for file entries before, move them here
		// FIXME: is an empty name a valid file name in a directory ?
		directoryChildrens = slices.Grow(directoryChildrens, len(fileChildrens)+extra)
		for _, v := range fileChildrens {
			directoryChildrens = append(directoryChildrens, DirectoryEntry[Children]{
				Entry: Entry[Children]{Cid: v.Cid, tSize: v.tSize},
				Name:  AliasableString{},
			})
		}

		fileChildrens = nil
	}

	for len(data) != 0 { // iterate at the root level of the message
		outerNumber, t, l := protowire.ConsumeTag(data)
		if l < 0 {
			return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
		}
		data = data[l:]
		switch outerNumber {
		case 1, 2:
			// optional Data Data = 1;
			// repeated PBLink Links = 2;
			var group bool
			var mData []byte
			switch t {
			case protowire.StartGroupType:
				// boundry delimited message
				group = true
				mData = data
			case protowire.BytesType:
				// length prefixed message
				mData, l = protowire.ConsumeBytes(data)
				if l < 0 {
					return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
				}
				data = data[l:] // we just extracted the message so walk over it completely
			default:
				var err error
				if outerNumber == 1 {
					err = fmt.Errorf("unknown type for Data field %v", t)
				} else {
					err = fmt.Errorf("unknown type for Links field %v", t)
				}
				return 0, nil, 0, 0, nil, nil, 0, err
			}

			var c cid.GenericCid[Children]
			var name []byte
			var tSize uint64 // will be offset by +1, zero means not found

			for len(mData) != 0 {
				n, t, l := protowire.ConsumeTag(mData)
				if l < 0 {
					return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
				}
				mData = mData[l:]

				if t == protowire.EndGroupType {
					// if we find an EGROUP here it must be ours since pbHandleUnknownField skip over groups.
					break
				}

				if outerNumber == 1 {
					// optional Data Data = 1;
					switch n {
					case 1:
						// required DataType Type = 1;
						var err error
						mData, dataType, err = pbDecodeNumber(t, mData)
						if err != nil {
							return 0, nil, 0, 0, nil, nil, 0, err
						}
						// due to how "Last One Wins" we can't do anything meaningfull without fully decoding the message first.

					case 2:
						// optional bytes Data = 2;
						switch t {
						case protowire.BytesType:
							content, l = protowire.ConsumeBytes(mData)
							if l < 0 {
								return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
							}
							mData = mData[l:]

						default:
							return 0, nil, 0, 0, nil, nil, 0, fmt.Errorf("unknown type for Data.Data field %v", t)
						}

					case 4:
						// repeated uint64 blocksizes = 4;
						addBlocksize := func(blocksize uint64) error {
							if len(directoryChildrens) != 0 {
								return errors.New("invalid unixfs node, mixed use of blocksizes and named links")
							}

							if uint(len(fileChildrens)) > blocksizes {
								// we have discovered more links than blocksizes at this point, play catchup
								fileChildrens[blocksizes].FileSize = blocksize
							} else {
								// we have discovered more blocksizes than links at this point, add new entries
								fileChildrens = append(fileChildrens, FileEntry[Children]{FileSize: blocksize})
							}
							blocksizes++
							return nil
						}

						switch t {
						// FIXME: this condition accepts Fixed numbers, is that valid ?
						//        I mean it works but do other protobuf parsers do this ?
						case protowire.VarintType, protowire.Fixed64Type, protowire.Fixed32Type:
							var blocksize uint64
							var err error
							mData, blocksize, err = pbDecodeNumber(t, mData)
							if err != nil {
								return 0, nil, 0, 0, nil, nil, 0, err
							}
							addBlocksize(blocksize)

						case protowire.BytesType:
							// packed representation
							packed, l := protowire.ConsumeBytes(mData)
							if l < 0 {
								return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
							}
							mData = mData[l:]

							for len(packed) != 0 {
								blocksize, l := protowire.ConsumeVarint(packed)
								if l < 0 {
									return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
								}
								packed = packed[l:]

								addBlocksize(blocksize)
							}

						default:
							return 0, nil, 0, 0, nil, nil, 0, fmt.Errorf("unknown type for Data.Blocksizes field %v", t)
						}

					default:
						var err error
						mData, err = pbHandleUnknownField(t, mData)
						if err != nil {
							return 0, nil, 0, 0, nil, nil, 0, err
						}
					}
				} else {
					// repeated PBLink Links = 2;
					switch n {
					case 1:
						// optional bytes Hash = 1;
						switch t {
						case protowire.BytesType:
							cBytes, l := protowire.ConsumeBytes(mData)
							if l < 0 {
								return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
							}
							mData = mData[l:]

							var err error
							c, err = cid.CastGeneric[Children](cBytes)
							if err != nil {
								return 0, nil, 0, 0, nil, nil, 0, fmt.Errorf("failed to decode cid: %w", err)
							}
						default:
							return 0, nil, 0, 0, nil, nil, 0, fmt.Errorf("unknown type for Links.Hash field %v", t)
						}

					case 2:
						// optional string Name = 2;
						switch t {
						case protowire.BytesType:
							name, l = protowire.ConsumeBytes(mData)
							if l < 0 {
								return 0, nil, 0, 0, nil, nil, 0, protowire.ParseError(l)
							}
							mData = mData[l:]

						default:
							return 0, nil, 0, 0, nil, nil, 0, fmt.Errorf("unknown type for Links.Name field %v", t)
						}

					case 3:
						// optional uint64 Tsize = 3;
						var err error
						mData, tSize, err = pbDecodeNumber(t, mData)
						if err != nil {
							return 0, nil, 0, 0, nil, nil, 0, err
						}
						if selfTSize != 0 {
							if tSize == 0 {
								selfTSize = 0
							} else {
								selfTSize += tSize
							}
						}
						tSize++

					default:
						var err error
						mData, err = pbHandleUnknownField(t, mData)
						if err != nil {
							return 0, nil, 0, 0, nil, nil, 0, err
						}
					}
				}
			}

			if outerNumber == 2 {
				// repeated PBLink Links = 2;
				if !c.Defined() {
					return 0, nil, 0, 0, nil, nil, 0, errors.New("link is missing CID")
				}

				// note we accept present but empty name entries on files because some historic
				// encoder emited a whole bunch of them in the wild
				if len(name) != 0 || len(directoryChildrens) != 0 {
					// Directory entry
					if blocksizes != 0 {
						return 0, nil, 0, 0, nil, nil, 0, errors.New("mixed use of blocksizes and named links")
					}

					if len(fileChildrens) != 0 {
						moveZeroNamedDirectoryEntriesToDirectoryChildrens(1)
					}

					directoryChildrens = append(directoryChildrens, DirectoryEntry[Children]{
						Entry: Entry[Children]{Cid: c, tSize: tSize},
						Name:  AliasableString(name),
					})
				} else {
					// File entry
					if uint(len(fileChildrens)) > fileLinks {
						// we have discovered more blocksizes than links at this point, play catchup
						fileChildrens[fileLinks].Cid = c
						fileChildrens[fileLinks].tSize = tSize
					} else {
						// we have discovered more links than blocksizes at this point, add new entries
						fileChildrens = append(fileChildrens, FileEntry[Children]{Entry: Entry[Children]{Cid: c, tSize: tSize}})
					}
					fileLinks++
				}
			}

			if group {
				// Now that we have found the end restore data.
				data = mData
			}

		default:
			var err error
			data, err = pbHandleUnknownField(t, data)
			if err != nil {
				return 0, nil, 0, 0, nil, nil, 0, err
			}
		}
	}

	return dataType, fileChildrens, fileLinks, blocksizes, directoryChildrens, content, selfTSize, nil
}

// pbHandleUnknownField must be called right after the tag, it will handle
// skipping uneeded values if needed.
func pbHandleUnknownField(t protowire.Type, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("no field to consume")
	}

	var groupStack uint
	for {
		var l int
		switch t {
		case protowire.BytesType:
			_, l = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			_, l = protowire.ConsumeVarint(data)
		case protowire.Fixed64Type:
			_, l = protowire.ConsumeFixed64(data)
		case protowire.Fixed32Type:
			_, l = protowire.ConsumeFixed32(data)
		case protowire.StartGroupType:
			groupStack++
			goto next
		case protowire.EndGroupType:
			if groupStack == 0 {
				return nil, errors.New("unmatched end group")
			}
			groupStack--
			goto next
		default:
			return nil, fmt.Errorf("unknown protobuf type: %v", t)
		}
		if l < 0 {
			return nil, protowire.ParseError(l)
		}
		data = data[l:]

	next:
		if groupStack == 0 {
			break
		}

		_, t, l = protowire.ConsumeTag(data)
		if l < 0 {
			return nil, protowire.ParseError(l)
		}
		data = data[l:]
	}
	return data, nil
}

// pbDecodeNumber will decode a uint64 as best as it can.
// It must be called right after the tag.
func pbDecodeNumber(typ protowire.Type, data []byte) ([]byte, uint64, error) {
	var v uint64
	var l int
	switch typ {
	case protowire.VarintType:
		v, l = protowire.ConsumeVarint(data)
	case protowire.Fixed64Type:
		v, l = protowire.ConsumeFixed64(data)
	case protowire.Fixed32Type:
		var v32 uint32
		v32, l = protowire.ConsumeFixed32(data)
		v = uint64(v32)
	default:
		return nil, 0, fmt.Errorf("unexpected type for number %v", typ)
	}
	if l < 0 {
		return nil, 0, protowire.ParseError(l)
	}
	return data[l:], v, nil
}
