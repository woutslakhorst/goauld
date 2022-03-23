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

// IndexOption is the option function for adding options to a FieldIndexer
type IndexOption func(fieldIndexer *fieldIndexer)

// TransformerOption is the option for a FieldIndexer to apply transformation before indexing the value.
// The transformation is also applied to a query value that matches the indexed field.
func TransformerOption(transformer Transform) IndexOption {
	return func(fieldIndexer *fieldIndexer) {
		fieldIndexer.transformer = transformer
	}
}

// TokenizerOption is the option for a FieldIndexer to split a value to be indexed into multiple parts.
// Each part is then indexed separately.
func TokenizerOption(tokenizer Tokenizer) IndexOption {
	return func(fieldIndexer *fieldIndexer) {
		fieldIndexer.tokenizer = tokenizer
	}
}

// IRIComparable defines if two structs can be compared on IRI terms.
type IRIComparable interface {
	// Equals returns true if the two IRIComparable have the same termPath (same IRI's in same order).
	Equals(other IRIComparable) bool
	// TermPath returns the TermPath
	TermPath() TermPath
}

// FieldIndexer is the public interface that defines functions for a field index instruction.
// A FieldIndexer is used when a document is indexed.
type FieldIndexer interface {
	IRIComparable
	// Tokenize may split up Keys and search terms. For example split a sentence into words.
	Tokenize(value Scalar) []Scalar
	// Transform is a function that alters the value to be indexed as well as any search criteria.
	// For example LowerCase is a Transform function that transforms the value to lower case.
	Transform(value Scalar) Scalar
}

// NewFieldIndexer creates a new fieldIndexer
// leave the name empty to use the json path as name.
// the name is to be used as query key when searching
func NewFieldIndexer(termPath TermPath, options ...IndexOption) FieldIndexer {
	fi := fieldIndexer{
		termPath: termPath,
	}
	for _, o := range options {
		o(&fi)
	}
	return fi
}

type fieldIndexer struct {
	termPath    TermPath
	transformer Transform
	tokenizer   Tokenizer
}

func (j fieldIndexer) Equals(other IRIComparable) bool {
	return j.termPath.Equals(other.TermPath())
}

func (j fieldIndexer) TermPath() TermPath {
	return j.termPath
}

func (j fieldIndexer) Tokenize(scalar Scalar) []Scalar {
	if j.tokenizer == nil {
		return []Scalar{scalar}
	}

	if s, ok := scalar.value.(string); ok {
		tokens := j.tokenizer(s)
		result := make([]Scalar, len(tokens))
		for i, t := range tokens {
			result[i] = ScalarMustParse(t)
		}
		return result
	}
	return []Scalar{scalar}
}

func (j fieldIndexer) Transform(value Scalar) Scalar {
	if j.transformer == nil {
		return value
	}
	return j.transformer(value)
}
