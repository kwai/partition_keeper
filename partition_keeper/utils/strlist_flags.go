package utils

import (
	"errors"
	"fmt"
	"strings"
)

type StrlistFlag []string

func (s *StrlistFlag) String() string {
	return fmt.Sprint(*s)
}

func (s *StrlistFlag) Set(value string) error {
	if len(*s) > 0 {
		return errors.New("str array flag already set")
	}
	*s = strings.Split(value, ",")
	return nil
}
