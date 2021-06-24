// package index provides indexing functionality for CAR v1 data payload represented as a mapping of
// CID to offset. This can then be used to implement random access over a CAR v1.
//
// Index can be written or read using the following static functions: index.WriteTo and
// index.ReadFrom.
//
// This package also provides functionality to generate an index from a given CAR v1 file using:
// index.Generate and index.GenerateFromFile
//
// In addition to the above, it provides functionality to attach an index to an index-less CAR v2
// using index.Attach
package index
