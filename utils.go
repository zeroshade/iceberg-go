// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

import (
	"cmp"
	"fmt"
	"maps"
	"runtime/debug"
	"strings"
	"unsafe"
)

var version string

func init() {
	version = "(unknown version)"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if strings.HasPrefix(dep.Path, "github.com/apache/iceberg-go") {
				version = dep.Version
				break
			}
		}
	}
}

func Version() string { return version }

func max[T cmp.Ordered](vals ...T) T {
	if len(vals) == 0 {
		panic("can't call max with no arguments")
	}

	out := vals[0]
	for _, v := range vals[1:] {
		if v > out {
			out = v
		}
	}
	return out
}

type Optional[T any] struct {
	Val   T
	Valid bool
}

type literalSet interface {
	literalSet()
}

type set[E any] interface {
	add(...E)
	contains(E) bool
	members() []E
	equals(set[E]) bool
	len() int

	literalSet()
}

type primitiveSet[E comparable] map[E]struct{}

func newPrimitiveSet[E comparable](vals ...E) set[E] {
	s := primitiveSet[E]{}
	for _, v := range vals {
		s[v] = struct{}{}
	}
	return s
}

func (s primitiveSet[E]) len() int    { return len(s) }
func (s primitiveSet[E]) literalSet() {}

func (s primitiveSet[E]) add(vals ...E) {
	for _, v := range vals {
		s[v] = struct{}{}
	}
}

func (s primitiveSet[E]) contains(v E) bool {
	_, ok := s[v]
	return ok
}

func (s primitiveSet[E]) members() []E {
	result := make([]E, 0, len(s))
	for v := range s {
		result = append(result, v)
	}
	return result
}

func (s primitiveSet[E]) equals(other set[E]) bool {
	rhs, ok := other.(primitiveSet[E])
	if !ok {
		return false
	}
	return maps.Equal(s, rhs)
}

type byteSliceSet map[string]struct{}

func newByteSliceSet(vals ...[]byte) byteSliceSet {
	s := byteSliceSet{}
	for _, v := range vals {
		s[s.getStr(v)] = struct{}{}
	}
	return s
}

func (byteSliceSet) literalSet() {}

func (byteSliceSet) getStr(v []byte) string {
	return unsafe.String(unsafe.SliceData(v), len(v))
}

func (bs byteSliceSet) add(vals ...[]byte) {
	for _, v := range vals {
		bs[bs.getStr(v)] = struct{}{}
	}
}

func (bs byteSliceSet) contains(v []byte) bool {
	_, ok := bs[bs.getStr(v)]
	return ok
}

func (bs byteSliceSet) members() [][]byte {
	result := make([][]byte, 0, len(bs))
	for v := range bs {
		result = append(result, unsafe.Slice(unsafe.StringData(v), len(v)))
	}
	return result
}

func (bs byteSliceSet) equals(other set[[]byte]) bool {
	rhs, ok := other.(byteSliceSet)
	if !ok {
		return false
	}

	return maps.Equal(bs, rhs)
}

func (bs byteSliceSet) len() int { return len(bs) }

type structLike interface {
	Size() int
	Get(pos int) any
	Set(pos int, val any)
}

type accessor struct {
	pos   int
	inner *accessor
}

func (a *accessor) String() string {
	return fmt.Sprintf("Accessor(position=%d,inner=%s)", a.pos, a.inner)
}

func (a *accessor) Get(structLike) any { return nil }
