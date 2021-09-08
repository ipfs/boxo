// Package options provides utilities to work with ReadOption and WriteOption used internally.
package options

import carv2 "github.com/ipld/go-car/v2"

// SplitReadWriteOptions splits the rwopts by type into ReadOption and WriteOption slices.
func SplitReadWriteOptions(rwopts ...carv2.ReadWriteOption) ([]carv2.ReadOption, []carv2.WriteOption) {
	var ropts []carv2.ReadOption
	var wopts []carv2.WriteOption
	for _, opt := range rwopts {
		switch opt := opt.(type) {
		case carv2.ReadOption:
			ropts = append(ropts, opt)
		case carv2.WriteOption:
			wopts = append(wopts, opt)
		}
	}
	return ropts, wopts
}
