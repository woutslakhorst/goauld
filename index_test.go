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

func TestIndex_AddJson(t *testing.T) {
	s := testStore(t)
	db := s.db
	c := createCollection(db)
	doc := jsonLdExample
	ref := defaultReferenceCreator(doc)
	nameTermPath := NewTermPath("http://schema.org/name")
	doc2 := jsonLdExample2
	ref2 := defaultReferenceCreator(doc2)

	t.Run("ok - value added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(nameTermPath))

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, []byte("Jane Doe"), ref)
	})

	t.Run("ok - values added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewTermPath("http://schema.org/url")),
			NewFieldIndexer(nameTermPath),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, ComposeKey(Key("http://www.janedoe.com"), Key("Jane Doe")), ref)
	})

	t.Run("ok - value added as key using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(nameTermPath),
			NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/name")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		key := ComposeKey(Key("Jane Doe"), Key("John Doe"))

		assertIndexed(t, db, i, key, ref)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewTermPath("http://schema.org/telephone")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		key := Key("(425) 123-4567")

		// check if both docs are indexed
		assertIndexed(t, db, i, key, ref)
		assertIndexed(t, db, i, key, ref2)
		assertIndexSize(t, db, i, 2)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(nameTermPath))

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, Document{})
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewTermPath("http://schema.org/image")))

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added with nil index value", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(nameTermPath),
			NewFieldIndexer(NewTermPath("http://schema.org/image")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			_ = i.Add(testBucket(t, tx), ref, doc)
			return i.Add(testBucket(t, tx), ref2, doc2)
		})

		key := ComposeKey(Key("Jane Doe"), []byte{})

		assertIndexed(t, db, i, key, ref)
		assertIndexSize(t, db, i, 2)
	})
}

func TestIndex_Delete(t *testing.T) {
	s := testStore(t)
	db := s.db
	c := createCollection(db)
	doc := jsonLdExample
	doc2 := jsonLdExample2
	ref := defaultReferenceCreator(doc)
	ref2 := defaultReferenceCreator(doc2)
	nameTermPath := NewTermPath("http://schema.org/name")

	t.Run("ok - value added and removed", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(nameTermPath))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added and removed using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(nameTermPath),
			NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/name")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(nameTermPath))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, doc2)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - not indexed", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/name")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(nameTermPath),
			NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/name")),
		)

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			_ = i.Add(b, ref2, doc2)
			return i.Delete(b, ref, doc)
		})

		if !assert.NoError(t, err) {
			return
		}

		key := ComposeKey(Key("John Doe"), Key{})

		assertIndexed(t, db, i, key, ref2)
	})
}

func TestIndex_IsMatch(t *testing.T) {
	s := testStore(t)
	db := s.db
	c := createCollection(db)
	i := c.NewIndex(t.Name(),
		NewFieldIndexer(NewTermPath("http://schema.org/name")),
		NewFieldIndexer(NewTermPath("http://schema.org/url")),
	)

	t.Run("ok - exact match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewTermPath("http://schema.org/name"), ScalarMustParse("Jane Doe"))).
				And(Eq(NewTermPath("http://schema.org/url"), ScalarMustParse("http://www.janedoe.com"))))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - exact match reverse ordering", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewTermPath("http://schema.org/url"), ScalarMustParse("http://www.janedoe.com"))).
				And(Eq(NewTermPath("http://schema.org/name"), ScalarMustParse("Jane Doe"))))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewTermPath("http://schema.org/name"), ScalarMustParse("Jane Doe"))))

		assert.Equal(t, 0.5, f)
	})

	t.Run("ok - no match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewTermPath("http://schema.org/weight"), ScalarMustParse("Jane Doe"))))

		assert.Equal(t, 0.0, f)
	})

	t.Run("ok - no match on second index only", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewTermPath("http://schema.org/url"), ScalarMustParse("http://www.janedoe.com"))))

		assert.Equal(t, 0.0, f)
	})
}

func TestIndex_Find(t *testing.T) {
	s := testStore(t)
	db := s.db
	c := createCollection(db)
	doc := jsonLdExample
	doc2 := jsonLdExample2
	ref := defaultReferenceCreator(doc)
	ref2 := defaultReferenceCreator(doc2)
	nameTermPath := NewTermPath("http://schema.org/name")
	urlTermPath := NewTermPath("http://schema.org/url")
	childTermPath := NewTermPath("http://schema.org/children", "http://schema.org/name")

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(nameTermPath, TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
		NewFieldIndexer(urlTermPath),
		NewFieldIndexer(childTermPath),
	)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		_ = i.Add(b, ref, doc)
		return i.Add(b, ref2, doc2)
	})

	t.Run("ok - not found", func(t *testing.T) {
		q := New(Eq(nameTermPath, ScalarMustParse("not_found")))
		found := false

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found = true
				return nil
			})
		})

		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("ok - exact match", func(t *testing.T) {
		q := New(Eq(nameTermPath, ScalarMustParse("Jane"))).And(
			Eq(urlTermPath, ScalarMustParse("http://www.janedoe.com"))).And(
			Eq(childTermPath, ScalarMustParse("John Doe")))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - match through transformer", func(t *testing.T) {
		q := New(Eq(nameTermPath, ScalarMustParse("JANE"))).And(
			Eq(urlTermPath, ScalarMustParse("http://www.janedoe.com"))).And(
			Eq(childTermPath, ScalarMustParse("John Doe")))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		q := New(Eq(nameTermPath, ScalarMustParse("Jane")))

		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - match with nil values at multiple levels", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)

		i := c.NewIndex(t.Name(),
			NewFieldIndexer(nameTermPath, TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
			NewFieldIndexer(NewTermPath("http://schema.org/unknown")),
			NewFieldIndexer(NewTermPath("http://schema.org/unknown2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		q := New(Eq(nameTermPath, ScalarMustParse("Jane")))

		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("error - wrong query", func(t *testing.T) {
		q := New(Eq(NewTermPath("http://schema.org/unknown"), ScalarMustParse("Jane Doe")))

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				return nil
			})
		})

		assert.Error(t, err)
	})
}

func TestIndex_addRefToBucket(t *testing.T) {
	t.Run("adding more than 16 entries", func(t *testing.T) {
		db := testDB(t)

		err := db.Update(func(tx *bbolt.Tx) error {
			bucket := testBucket(t, tx)

			for i := uint32(0); i < 16; i++ {
				iBytes, _ := toBytes(i)
				if err := addRefToBucket(bucket, []byte("key"), iBytes); err != nil {
					return err
				}
			}

			return nil
		})

		assert.NoError(t, err)

		// stats are not updated until after commit
		_ = db.View(func(tx *bbolt.Tx) error {
			bucket := testBucket(t, tx)
			b := bucket.Bucket([]byte("key"))

			assert.NotNil(t, b)
			assert.Equal(t, 16, b.Stats().KeyN)

			return nil
		})
	})
}

func TestIndex_Sort(t *testing.T) {
	nameTermPath := NewTermPath("http://schema.org/name")
	childTermPath := NewTermPath("http://schema.org/children", "http://schema.org/name")
	s := ScalarMustParse("value")
	db := testDB(t)
	c := createCollection(db)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(nameTermPath),
		NewFieldIndexer(childTermPath),
	)

	t.Run("returns correct order when given in reverse", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq(childTermPath, s)).
				And(Eq(nameTermPath, s)), false)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, nameTermPath.Head(), sorted[0].TermPath().Head())
		assert.Equal(t, childTermPath.Head(), sorted[1].TermPath().Head())
	})

	t.Run("returns correct order when given in correct order", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq(nameTermPath, s)).
				And(Eq(childTermPath, s)), false)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, nameTermPath.Head(), sorted[0].TermPath().Head())
		assert.Equal(t, childTermPath.Head(), sorted[1].TermPath().Head())
	})

	t.Run("does not include any keys when primary key is missing", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq(childTermPath, s)), false)

		assert.Len(t, sorted, 0)
	})

	t.Run("includes all keys when includeMissing option is given", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq(NewTermPath("http://schema.org/url"), s)).
				And(Eq(childTermPath, s)), true)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, "http://schema.org/url", sorted[0].TermPath().Head())
		assert.Equal(t, "http://schema.org/children", sorted[1].TermPath().Head())
	})

	t.Run("includes additional keys when includeMissing option is given", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq(NewTermPath("http://schema.org/url"), s)).
				And(Eq(nameTermPath, s)), true)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, nameTermPath.Head(), sorted[0].TermPath().Head())
		assert.Equal(t, "http://schema.org/url", sorted[1].TermPath().Head())
	})
}

func TestIndex_QueryPartsOutsideIndex(t *testing.T) {
	nameTermPath := NewTermPath("http://schema.org/name")
	childTermPath := NewTermPath("http://schema.org/children", "http://schema.org/name")
	s := ScalarMustParse("value")
	db := testDB(t)
	c := createCollection(db)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(nameTermPath),
		NewFieldIndexer(childTermPath),
	)

	t.Run("returns empty list when all parts in index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(childTermPath, s)).
				And(Eq(nameTermPath, s)))

		assert.Len(t, additional, 0)
	})

	t.Run("returns all parts when none match index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(childTermPath, s)))

		assert.Len(t, additional, 1)
	})

	t.Run("returns correct params on partial index match", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(NewTermPath("http://example.org/url"), s)).
				And(Eq(nameTermPath, s)))

		if !assert.Len(t, additional, 1) {
			return
		}
		assert.Equal(t, "http://example.org/url", additional[0].TermPath().Head())
	})
}

func TestIndex_Keys(t *testing.T) {
	s := testStore(t)
	c := createCollection(s.db)
	i := testIndex(t, c)

	t.Run("ok - sub object", func(t *testing.T) {
		ip := NewFieldIndexer(NewTermPath("http://schema.org/name"))

		keys, err := i.Keys(ip, jsonLdExample)

		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, keys, 1) {
			return
		}
		assert.Equal(t, "Jane Doe", keys[0].value)
	})

	t.Run("ok - sub sub object", func(t *testing.T) {
		ip := NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/name"))

		keys, err := i.Keys(ip, jsonLdExample)

		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, keys, 1) {
			return
		}
		assert.Equal(t, "John Doe", keys[0].value)
	})

	t.Run("ok - no match", func(t *testing.T) {
		ip := NewFieldIndexer(NewTermPath("http://schema.org/children", "http://schema.org/url"))

		keys, err := i.Keys(ip, jsonLdExample)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, keys, 0)
	})
}
