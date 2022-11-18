package delegatedrouting

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestThing(t *testing.T) {
	r := BitswapReadProviderRecord{
		TransferProtocol: TransferProtocol{Protocol: "proto"},
	}
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", string(b))
}
