package hosts

import (
	"encoding/json"
	"net"
)

// IPNet is a net.IPNet wrapped to add marshaling.
type IPNet net.IPNet

// String implements fmt.Stringer.
func (n IPNet) String() string {
	subnet := net.IPNet(n)
	return subnet.String()
}

// MarshalText implements encoding.TextMarshaler.
func (n IPNet) MarshalText() ([]byte, error) {
	return []byte(n.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *IPNet) UnmarshalText(text []byte) error {
	ip, subnet, err := net.ParseCIDR(string(text))
	if err != nil {
		return err
	}
	if ip != nil {
		subnet.IP = ip
		if ip4 := subnet.IP.To4(); ip4 != nil {
			subnet.IP = ip4
		}
	}
	*n = IPNet(*subnet)
	return nil
}

// MarshalJSON implements json.Marshaler for Host.
func (h Host) MarshalJSON() ([]byte, error) {
	type Alias Host
	return json.Marshal(&struct {
		Networks []IPNet `json:"networks"`
		*Alias
	}{
		Networks: func() []IPNet {
			out := make([]IPNet, len(h.Networks))
			for i, n := range h.Networks {
				out[i] = IPNet(n)
			}
			return out
		}(),
		Alias: (*Alias)(&h),
	})
}

// UnmarshalJSON implements json.Unmarshaler for Host.
func (h *Host) UnmarshalJSON(data []byte) error {
	type Alias Host
	aux := &struct {
		Networks []IPNet `json:"networks"`
		*Alias
	}{
		Alias: (*Alias)(h),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	h.Networks = make([]net.IPNet, len(aux.Networks))
	for i, n := range aux.Networks {
		h.Networks[i] = net.IPNet(n)
	}
	return nil
}
