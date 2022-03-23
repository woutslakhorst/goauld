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
	"go.etcd.io/bbolt"
)

// this file tests indexing and finding using a transformer
// to test the adding and searching capabilities, we use a string based document.
// the test based index returns each word as a key.

type testIndexPart struct {
	termPath    TermPath
	transformer Transform
	tokenizer   Tokenizer
}

func (t testIndexPart) Equals(other IRIComparable) bool {
	return t.termPath.Equals(other.TermPath())
}

func (t testIndexPart) TermPath() TermPath {
	return t.termPath
}

func (t testIndexPart) Keys(document Document) ([]Scalar, error) {
	words := t.tokenizer(string(document))
	keys := make([]Scalar, len(words))
	for i, w := range words {
		keys[i] = t.Transform(ScalarMustParse(w))
	}
	return keys, nil
}

func (t testIndexPart) Tokenize(value Scalar) []Scalar {
	if t.tokenizer == nil {
		return []Scalar{value}
	}
	if s, ok := value.value.(string); ok {
		tokens := t.tokenizer(s)
		result := make([]Scalar, len(tokens))
		for i, t := range tokens {
			result[i] = ScalarMustParse(t)
		}
		return result
	}
	return []Scalar{value}
}

func (t testIndexPart) Transform(value Scalar) Scalar {
	if t.transformer == nil {
		return value
	}
	return t.transformer(value)
}

func (t testIndexPart) Transformer() Transform {
	return t.transformer
}

func TestIndex_Add(t *testing.T) {
	db := testDB(t)
	c := createCollection(db)

	t.Run("with tokenizer", func(t *testing.T) {
		t.Run("ok - single word", func(t *testing.T) {
			i := c.NewIndex("test", testIndexPart{termPath: NewTermPath("http://schema.org/url"), tokenizer: WhiteSpaceTokenizer, transformer: ToLower})
			ref := []byte("01")
			key := []byte("http://www.janedoe.com")

			err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
				return i.Add(bucket, ref, jsonLdExample)
			})

			assert.NoError(t, err)

			assertIndexed(t, db, i, key, ref)
		})

		t.Run("ok - sentence", func(t *testing.T) {
			i := c.NewIndex("test", testIndexPart{termPath: NewTermPath("http://schema.org/name"), tokenizer: WhiteSpaceTokenizer, transformer: ToLower})
			ref := []byte("01")
			key1 := []byte("jane")
			key2 := []byte("doe")

			err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
				return i.Add(bucket, ref, jsonLdExample)
			})

			assert.NoError(t, err)

			assertIndexed(t, db, i, key1, ref)
			assertIndexed(t, db, i, key2, ref)
		})
	})
}

func TestIndex_Iterate(t *testing.T) {
	t.Run("ok - single word", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		i := c.NewIndex("test", testIndexPart{termPath: NewTermPath("http://schema.org/url"), tokenizer: WhiteSpaceTokenizer, transformer: ToLower})

		ref := []byte("01")
		key := ScalarMustParse("http://www.janedoe.com")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, jsonLdExample)
		})

		if !assert.NoError(t, err) {
			return
		}

		q := New(Eq(NewTermPath("http://schema.org/url"), key))
		count := 0

		err = withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Iterate(bucket, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - sentence", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		i := c.NewIndex("test", testIndexPart{termPath: NewTermPath("http://schema.org/name"), tokenizer: WhiteSpaceTokenizer, transformer: ToLower})

		ref := []byte("01")
		key2 := ScalarMustParse("doe")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, jsonLdExample)
		})

		if !assert.NoError(t, err) {
			return
		}

		q := New(Eq(NewTermPath("http://schema.org/name"), key2))
		count := 0

		err = withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Iterate(bucket, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestWhiteSpaceTokenizer(t *testing.T) {
	t.Run("ok - consecutive whitespace", func(t *testing.T) {
		tokens := WhiteSpaceTokenizer("WORD1 WORD2")

		assert.Len(t, tokens, 2)
	})
}
