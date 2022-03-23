/*
 * go-leia
 * Copyright (C) 2021 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package goauld

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
)

const boltDBFileMode = 0600
const KeyDelimiter = 0x10

// Reference equals a document hash. In an index, the values are references to docs.
type Reference []byte

// ErrInvalidJSON is returned when invalid JSON is parsed
var ErrInvalidJSON = errors.New("invalid json")

// Document is the byte slice representation of a document
type Document []byte

// EncodeToString encodes the reference as hex encoded string
func (r Reference) EncodeToString() string {
	return hex.EncodeToString(r)
}

// ByteSize returns the size of the reference, eg: 32 bytes for a sha256
func (r Reference) ByteSize() int {
	return len(r)
}

// TermPath represents a nested term structure (or graph path) using the fully qualified IRIs
type TermPath struct {
	// Terms represent the nested structure from highest (index 0) to lowest nesting
	Terms []string
}

func NewTermPath(terms ...string) TermPath {
	return TermPath{Terms: terms}
}

// IsEmpty returns true of no terms are in the list
func (tp TermPath) IsEmpty() bool {
	return len(tp.Terms) == 0
}

// Head returns the first term of the list or ""
func (tp TermPath) Head() string {
	if len(tp.Terms) == 0 {
		return ""
	}
	return tp.Terms[0]
}

// Tail returns the last terms of the list or an empty TermPath
func (tp TermPath) Tail() TermPath {
	if len(tp.Terms) <= 1 {
		return TermPath{}
	}
	return TermPath{Terms: tp.Terms[1:]}
}

// Equals returns true if two TermPaths have the exact same Terms in the exact same order
func (tp TermPath) Equals(other TermPath) bool {
	if len(tp.Terms) != len(other.Terms) {
		return false
	}

	for i, term := range tp.Terms {
		if term != other.Terms[i] {
			return false
		}
	}
	return true
}

// Scalar represents a JSON-LD scalar (string, number, true or false)
type Scalar struct {
	value interface{}
}

// ErrInvalidValue is returned when an invalid value is parsed
var ErrInvalidValue = errors.New("invalid value")

// ScalarParse returns a Scalar based on an interface value. It returns ErrInvalidValue for unsupported values.
func ScalarParse(value interface{}) (Scalar, error) {
	switch value.(type) {
	case bool:
		return Scalar{value: value}, nil
	case string:
		return Scalar{value: value}, nil
	case float64:
		return Scalar{value: value}, nil
	}
	// not possible
	return Scalar{}, ErrInvalidValue
}

// ScalarMustParse returns a Scalar based on an interface value. It panics when the value is not supported.
func ScalarMustParse(value interface{}) Scalar {
	s, err := ScalarParse(value)
	if err != nil {
		panic(err)
	}
	return s
}

func (s Scalar) Bytes() []byte {
	switch castData := s.value.(type) {
	case bool:
		if castData {
			return []byte{1}
		}
		return []byte{0}
	case string:
		return []byte(castData)
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(castData))
		return buf[:]
	}

	return []byte{}
}

func toBytes(data interface{}) ([]byte, error) {
	switch castData := data.(type) {
	case []uint8:
		return castData, nil
	case uint32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], castData)
		return buf[:], nil
	case string:
		return []byte(castData), nil
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(castData))
		return buf[:], nil
	}

	return nil, errors.New("couldn't convert data to []byte")
}
