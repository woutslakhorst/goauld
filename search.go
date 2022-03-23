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
	"bytes"
	"errors"
)

// ErrNoQuery is returned when an empty query is given
var ErrNoQuery = errors.New("no query given")

type Query interface {
	// And adds a condition to query on
	And(part QueryPart) Query

	// Parts returns the different parts of the query
	Parts() []QueryPart
}

type QueryPart interface {
	IRIComparable
	// Seek returns the scalar for cursor.Seek
	Seek() Scalar
	// Condition returns true if given key falls within this condition.
	// The optional transform fn is applied to this query part before evaluation is done.
	Condition(value Key, transform Transform) bool
}

// New creates a new query with an initial query part. Both begin and end are inclusive for the conditional check.
func New(part QueryPart) Query {
	return query{
		parts: []QueryPart{part},
	}
}

// Eq creates a query part for an exact match
func Eq(termPath TermPath, value Scalar) QueryPart {
	return eqPart{
		termPath: termPath,
		value:    value,
	}
}

// Range creates a query part for a range query
func Range(termPath TermPath, begin Scalar, end Scalar) QueryPart {
	return rangePart{
		termPath: termPath,
		begin:    begin,
		end:      end,
	}
}

// Prefix creates a query part for a partial match
// The beginning of a value is matched against the query.
func Prefix(termPath TermPath, value Scalar) QueryPart {
	return prefixPart{
		termPath: termPath,
		value:    value,
	}
}

type query struct {
	parts []QueryPart
}

func (q query) And(part QueryPart) Query {
	q.parts = append(q.parts, part)
	return q
}

func (q query) Parts() []QueryPart {
	return q.parts
}

type eqPart struct {
	termPath TermPath
	value    Scalar
}

func (e eqPart) Equals(other IRIComparable) bool {
	return e.termPath.Equals(other.TermPath())
}

func (e eqPart) TermPath() TermPath {
	return e.termPath
}

func (e eqPart) Seek() Scalar {
	return e.value
}

func (e eqPart) Condition(key Key, transform Transform) bool {
	if transform != nil {
		transformed := transform(e.value)
		return bytes.Compare(key, transformed.Bytes()) == 0
	}

	return bytes.Compare(key, e.value.Bytes()) == 0
}

type rangePart struct {
	termPath TermPath
	begin    Scalar
	end      Scalar
}

func (r rangePart) Equals(other IRIComparable) bool {
	return r.termPath.Equals(other.TermPath())
}

func (r rangePart) TermPath() TermPath {
	return r.termPath
}

func (r rangePart) Seek() Scalar {
	return r.begin
}

func (r rangePart) Condition(key Key, transform Transform) bool {
	bTransformed := r.begin
	if transform != nil {
		bTransformed = transform(r.begin)
	}
	eTransformed := r.end
	if transform != nil {
		eTransformed = transform(r.end)
	}

	// the key becomes before the start
	if bytes.Compare(key, bTransformed.Bytes()) < 0 {
		return false
	}

	return bytes.Compare(key, eTransformed.Bytes()) <= 0
}

type prefixPart struct {
	termPath TermPath
	value    Scalar
}

func (p prefixPart) Equals(other IRIComparable) bool {
	return p.termPath.Equals(other.TermPath())
}

func (p prefixPart) TermPath() TermPath {
	return p.termPath
}

func (p prefixPart) Seek() Scalar {
	return p.value
}

func (p prefixPart) Condition(key Key, transform Transform) bool {
	transformed := p.value
	if transform != nil {
		transformed = transform(p.value)
	}

	return bytes.HasPrefix(key, transformed.Bytes())
}
