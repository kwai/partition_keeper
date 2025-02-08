package rodis

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	payloadTypes = map[string]bool{
		"KeyValue":      true,
		"Counter":       true,
		"TimeList":      true,
		"TagList":       true,
		"DedupTimeList": true,
		"TagMap":        true,
		"CounterMap":    true,
	}
)

type Payload struct {
	Name                string `json:"name"`
	Id                  *uint  `json:"id"`
	Type                string `json:"type"`
	Keyttl              uint   `json:"key_ttl"`
	Maxlen              *uint  `json:"max_len"`
	Itemttl             *uint  `json:"item_ttl"`
	ExpiredTruncatedLen *uint  `json:"expired_truncated_len"`
}

type DomainPayloads struct {
	Name     string     `json:"name"`
	Note     string     `json:"note"`
	Payloads []*Payload `json:"payload"`
}

func ParsePayloads(data []byte) (*DomainPayloads, error) {
	payloads := &DomainPayloads{}
	err := json.Unmarshal(data, payloads)
	if err != nil {
		return nil, err
	}
	if payloads.Name == "" {
		return nil, errors.New("domain name shouldn't be empty")
	}
	for i, payload := range payloads.Payloads {
		if payload.Id == nil {
			return nil, fmt.Errorf("domain %s %dth payload no id", payload.Name, i)
		}
		if payload.Name == "" {
			return nil, fmt.Errorf(
				"domain %s %dth payload, id %d name empty",
				payload.Name,
				i,
				*payload.Id,
			)
		}
		if _, ok := payloadTypes[payload.Type]; !ok {
			return nil, fmt.Errorf(
				"domain %s %dth payload, id %d can't recognize %s as a payload type",
				payload.Name,
				i,
				*payload.Id,
				payload.Type,
			)
		}
	}
	return payloads, nil
}
