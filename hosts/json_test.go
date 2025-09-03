package hosts

import (
	"encoding/json"
	"net"
	"reflect"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
)

func TestHostJSON(t *testing.T) {
	expected := Host{
		PublicKey: types.GeneratePrivateKey().PublicKey(),
		Networks: []net.IPNet{
			{IP: net.IP{127, 0, 0, 1}, Mask: net.CIDRMask(24, 32)},
			{IP: net.IP{192, 168, 1, 1}, Mask: net.CIDRMask(24, 32)},
		},
		Addresses: []chain.NetAddress{{Protocol: siamux.Protocol, Address: "host1.com"}},
		Settings: proto.HostSettings{
			AcceptingContracts: true,
			RemainingStorage:   123,
			Prices: proto.HostPrices{
				ContractPrice: types.Siacoins(1),
				Collateral:    types.NewCurrency64(1),
				StoragePrice:  types.NewCurrency64(1),
			},
			MaxContractDuration: 90 * 144,
			MaxCollateral:       types.Siacoins(1000),
		},
		Usability: GoodUsability,
	}

	encoded, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	var got Host
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}
}
