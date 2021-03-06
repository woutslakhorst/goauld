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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var jsonLdExample = []byte(`
{
  "@context": ["http://schema.org/",
  {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
	"alive": {"@id": "alive", "@type": "xsd:boolean"}
  }],
  "@type": "Person",
  "name": "Jane Doe",
  "jobTitle": "Professor",
  "telephone": "(425) 123-4567",
  "url": "http://www.janedoe.com",
  "weight": 80,
  "alive": true,
  "children": [
    {
      "name": "John Doe"
    }
  ]
}
`)

var jsonLdExample2 = []byte(`
{
  "@context": ["http://schema.org/"],
  "@type": "Person",
  "name": "John Doe",
  "jobTitle": "Soldier",
  "telephone": "(425) 123-4567",
  "weight": 90
}
`)

var invalidPathCharRegex = regexp.MustCompile("([^a-zA-Z0-9])")

// testDirectory returns a temporary directory for this test only. Calling TestDirectory multiple times for the same
// instance of t returns a new directory every time.
func testDirectory(t *testing.T) string {
	if dir, err := ioutil.TempDir("", normalizeTestName(t)); err != nil {
		t.Fatal(err)
		return ""
	} else {
		t.Cleanup(func() {
			if err := os.RemoveAll(dir); err != nil {
				_, _ = os.Stderr.WriteString(fmt.Sprintf("Unable to remove temporary directory for test (%s): %v\n", dir, err))
			}
		})
		return dir
	}
}

type testFunc func(bucket *bbolt.Bucket) error

func withinBucket(t *testing.T, db *bbolt.DB, fn testFunc) error {
	return db.Update(func(tx *bbolt.Tx) error {
		bucket := testBucket(t, tx)
		return fn(bucket)
	})
}

func testDB(t *testing.T) *bbolt.DB {
	db, err := bbolt.Open(filepath.Join(testDirectory(t), "test.db"), boltDBFileMode, &bbolt.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testStore(t *testing.T) *store {
	s, err := NewStore(filepath.Join(testDirectory(t), "test.db"), WithoutSync())
	if err != nil {
		t.Fatal(err)
	}
	return s.(*store)
}

func testBucket(t *testing.T, tx *bbolt.Tx) *bbolt.Bucket {
	if tx.Writable() {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		return bucket
	}
	return tx.Bucket([]byte("test"))
}

func normalizeTestName(t *testing.T) string {
	return invalidPathCharRegex.ReplaceAllString(t.Name(), "_")
}

// assertIndexed checks if a key/value has been indexed
func assertIndexed(t *testing.T, db *bbolt.DB, i Index, key []byte, ref Reference) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		b = b.Bucket(i.BucketName())
		sub := b.Bucket(key)

		cursor := sub.Cursor()
		for k, _ := cursor.Seek([]byte{}); k != nil; k, _ = cursor.Next() {
			if bytes.Compare(ref, k) == 0 {
				return nil
			}
		}

		return errors.New("ref not found")
	})

	return assert.NoError(t, err)
}

// assertIndexSize checks if an index has a certain size
func assertIndexSize(t *testing.T, db *bbolt.DB, i Index, size int) bool {
	err := db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		b = b.Bucket(i.BucketName())

		if b == nil {
			if size == 0 {
				return nil
			}
			return errors.New("empty bucket")
		}
		count := 0
		// loop over sub-buckets
		cursor := b.Cursor()
		for k, _ := cursor.Seek([]byte{}); k != nil; k, _ = cursor.Next() {
			subBucket := b.Bucket(k)
			subCursor := subBucket.Cursor()
			for k2, _ := subCursor.Seek([]byte{}); k2 != nil; k2, _ = subCursor.Next() {
				count++
			}
		}

		assert.Equal(t, size, count)
		return nil
	})

	return assert.NoError(t, err)
}

// assertSize checks a bucket size
func assertSize(t *testing.T, db *bbolt.DB, bucketName string, size int) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			if size == 0 {
				return nil
			}
			panic("missing bucket")
		}
		b = b.Bucket([]byte(bucketName))
		if b == nil {
			if size == 0 {
				return nil
			}
			panic("missing bucket")
		}
		assert.Equal(t, size, b.Stats().KeyN)
		return nil
	})

	return assert.NoError(t, err)
}
