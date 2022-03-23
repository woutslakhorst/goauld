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
	"testing"

	"github.com/stretchr/testify/assert"
)

var testSearchTerm = ScalarMustParse("test")
var testTermPath = NewTermPath("test")

func TestNew(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		q := New(Eq(testTermPath, testSearchTerm))

		assert.Len(t, q.Parts(), 1)
	})
}

func TestQuery_And(t *testing.T) {
	q := New(Eq(testTermPath, testSearchTerm))

	t.Run("ok", func(t *testing.T) {
		q = q.And(Eq(testTermPath, testSearchTerm))

		assert.Len(t, q.Parts(), 2)
	})
}

func TestEq(t *testing.T) {
	qp := Eq(testTermPath, testSearchTerm)

	t.Run("ok - TermPath", func(t *testing.T) {
		assert.Equal(t, "test", qp.TermPath().Head())
	})

	t.Run("ok - seek", func(t *testing.T) {
		s := qp.Seek()

		assert.Equal(t, "test", s.value)
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c := qp.Condition(Key("test"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("test2"), nil)

		assert.False(t, c)
	})
}

func TestRange(t *testing.T) {
	qp := Range(testTermPath, ScalarMustParse("a"), ScalarMustParse("b"))

	t.Run("ok - TermPath", func(t *testing.T) {
		assert.Equal(t, "test", qp.TermPath().Head())
	})

	t.Run("ok - seek", func(t *testing.T) {
		s := qp.Seek()

		assert.Equal(t, "a", s.value)
	})

	t.Run("ok - condition true begin", func(t *testing.T) {
		c := qp.Condition(Key("a"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true middle", func(t *testing.T) {
		c := qp.Condition(Key("ab"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true end", func(t *testing.T) {
		c := qp.Condition(Key("b"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("bb"), nil)

		assert.False(t, c)
	})
}

func TestPrefix(t *testing.T) {
	qp := Prefix(testTermPath, testSearchTerm)

	t.Run("ok - TermPath", func(t *testing.T) {
		assert.Equal(t, "test", qp.TermPath().Head())
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c := qp.Condition(Key("test something"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true with transform", func(t *testing.T) {
		qp := Prefix(testTermPath, ScalarMustParse("TEST"))
		c := qp.Condition(Key("test something"), ToLower)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("is not test"), nil)

		assert.False(t, c)
	})

	t.Run("ok - key too short", func(t *testing.T) {
		c := qp.Condition(Key("te"), nil)

		assert.False(t, c)
	})
}
