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
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestCollection_AddIndex(t *testing.T) {
	s := testStore(t)
	t.Run("ok", func(t *testing.T) {
		c := createCollection(s.db)
		i := testIndex(t, c)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.IndexList, 1)
	})

	t.Run("ok - duplicate", func(t *testing.T) {
		c := createCollection(s.db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.IndexList, 1)
	})

	t.Run("ok - new index adds refs", func(t *testing.T) {
		c := createCollection(s.db)
		i := testIndex(t, c)
		err := c.Add([]Document{jsonLdExample})
		assert.NoError(t, err)
		err = c.AddIndex(i)
		assert.NoError(t, err)

		assertIndexSize(t, s.db, i, 1)
		assertSize(t, s.db, documentBucket, 1)
	})

	t.Run("ok - adding existing index does nothing", func(t *testing.T) {
		c := createCollection(s.db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})

		assertIndexSize(t, s.db, i, 1)

		c2 := createCollection(s.db)
		_ = c2.AddIndex(i)

		assertIndexSize(t, s.db, i, 1)
	})
}

func TestCollection_DropIndex(t *testing.T) {
	db := testDB(t)

	t.Run("ok - dropping index removes refs", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.Add([]Document{jsonLdExample})
		_ = c.AddIndex(i)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - dropping index leaves other indices at rest", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.Add([]Document{jsonLdExample})
		i2 := c.NewIndex("other", NewFieldIndexer(NewTermPath("http://schema.org/name")))
		_ = c.AddIndex(i)
		_ = c.AddIndex(i2)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i2, 1)
	})
}

func TestCollection_Add(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		err := c.Add([]Document{jsonLdExample})
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, documentBucket, 1)
	})
}

func TestCollection_Delete(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})

		err := c.Delete(jsonLdExample)
		if !assert.NoError(t, err) {
			return
		}

		assertIndexSize(t, db, i, 0)
		assertSize(t, db, documentBucket, 0)
	})

	t.Run("ok - not added", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)

		err := c.Delete(jsonLdExample)
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, documentBucket, 0)
	})
}

func TestCollection_Find(t *testing.T) {
	db := testDB(t)
	nameTermPath := NewTermPath("http://schema.org/name")
	urlTermPath := NewTermPath("http://schema.org/url")
	janeDoe := ScalarMustParse("Jane Doe")
	janeDoeDotCom := ScalarMustParse("http://www.janedoe.com")

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe)).And(Eq(urlTermPath, janeDoeDotCom))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with Full table scan", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(urlTermPath, janeDoeDotCom))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan and range query", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe)).And(Range(NewTermPath("http://schema.org/weight"), ScalarMustParse(70.0), ScalarMustParse(90.0)))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan, range query not found", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe)).And(Range(NewTermPath("http://schema.org/weight"), ScalarMustParse(70.0), ScalarMustParse(79.0)))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("ok - no docs", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		q := New(Eq(nameTermPath, janeDoe))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("error - nil query", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})

		_, err := c.Find(context.TODO(), nil)

		assert.Error(t, err)
	})

	t.Run("error - ctx cancelled", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe))
		ctx, cancelFn := context.WithCancel(context.Background())

		cancelFn()
		_, err := c.Find(ctx, q)

		if !assert.Error(t, err) {
			return
		}

		assert.Equal(t, context.Canceled, err)
	})

	t.Run("error - deadline exceeded", func(t *testing.T) {
		c := createCollection(db)
		i := testIndex(t, c)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{jsonLdExample})
		q := New(Eq(nameTermPath, janeDoe))
		ctx, _ := context.WithTimeout(context.Background(), time.Nanosecond)

		_, err := c.Find(ctx, q)

		if !assert.Error(t, err) {
			return
		}

		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

func TestCollection_Iterate(t *testing.T) {
	s := testStore(t)
	c := createCollection(s.db)
	i := testIndex(t, c)
	_ = c.AddIndex(i)
	_ = c.Add([]Document{jsonLdExample})
	q := New(Eq(NewTermPath("http://schema.org/name"), ScalarMustParse("Jane Doe")))

	t.Run("ok - count fn", func(t *testing.T) {
		count := 0

		err := c.Iterate(q, func(key Reference, value []byte) error {
			count++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("error", func(t *testing.T) {
		err := c.Iterate(q, func(key Reference, value []byte) error {
			return errors.New("b00m!")
		})

		assert.Error(t, err)
	})
}

func TestCollection_IndexIterate(t *testing.T) {
	s := testStore(t)
	c := createCollection(s.db)
	i := testIndex(t, c)
	_ = c.AddIndex(i)
	_ = c.Add([]Document{jsonLdExample})
	q := New(Eq(NewTermPath("http://schema.org/name"), ScalarMustParse("Jane Doe")))

	t.Run("ok - count fn", func(t *testing.T) {
		count := 0

		err := s.db.View(func(tx *bbolt.Tx) error {
			return c.IndexIterate(q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("error", func(t *testing.T) {
		err := s.db.View(func(tx *bbolt.Tx) error {
			return c.IndexIterate(q, func(key []byte, value []byte) error {
				return errors.New("b00m!")
			})
		})

		assert.Error(t, err)
	})
}

func TestCollection_Reference(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)

		ref := c.Reference(jsonLdExample)

		assert.Equal(t, "829ea7a03cb0c7b30fd84df541ac458cc20dd478", ref.EncodeToString())
	})
}

func TestCollection_Get(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		ref := defaultReferenceCreator(jsonLdExample)
		if err := c.Add([]Document{jsonLdExample}); err != nil {
			t.Fatal(err)
		}

		d, err := c.Get(ref)

		if !assert.NoError(t, err) {
			return
		}

		if assert.NotNil(t, d) {
			assert.Equal(t, Document(jsonLdExample), d)
		}
	})

	t.Run("error - not found", func(t *testing.T) {
		c := createCollection(db)

		d, err := c.Get([]byte("test"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Nil(t, d)
	})
}

func TestCollection_ValuesAtPath(t *testing.T) {
	db := testDB(t)
	c := createCollection(db)

	t.Run("ok - find a single string value", func(t *testing.T) {
		values, err := c.ValuesAtPath(jsonLdExample, NewTermPath("http://schema.org/name"))

		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, values, 1)
		assert.Equal(t, "Jane Doe", string(values[0].Bytes()))
	})

	t.Run("ok - find a single boolean value", func(t *testing.T) {
		values, err := c.ValuesAtPath(jsonLdExample, NewTermPath("http://schema.org/alive"))

		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, values, 1)
		assert.Equal(t, []byte{1}, values[0].Bytes())
	})

	t.Run("ok - find a single number value", func(t *testing.T) {
		values, err := c.ValuesAtPath(jsonLdExample, NewTermPath("http://schema.org/weight"))

		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, values, 1)
		bits := binary.BigEndian.Uint64(values[0].Bytes())
		fl := math.Float64frombits(bits)
		assert.Equal(t, 80.0, fl)
	})

	t.Run("ok - find through a nested value", func(t *testing.T) {
		values, err := c.ValuesAtPath(jsonLdExample, NewTermPath("http://schema.org/children", "http://schema.org/name"))

		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, values, 1)
		assert.Equal(t, "John Doe", string(values[0].Bytes()))
	})
}

func TestNewIndex(t *testing.T) {
	db := testDB(t)
	c := createCollection(db)
	i := c.NewIndex("name")

	assert.Equal(t, "name", i.Name())
	assert.Len(t, i.(*index).indexParts, 0)
}

func testIndex(t *testing.T, collection Collection) Index {
	return collection.NewIndex(t.Name(),
		NewFieldIndexer(NewTermPath("http://schema.org/name")),
	)
}

func createCollection(db *bbolt.DB) *collection {
	return &collection{
		Name:      "test",
		db:        db,
		IndexList: []Index{},
		refMake:   defaultReferenceCreator,
	}
}
