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
	"maps"
	"runtime/debug"
	"strings"
	"unsafe"

	"golang.org/x/exp/constraints"
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

func max[T constraints.Ordered](vals ...T) T {
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

type accessor[T any] interface {
	Get(T) any
	Type() Type
}

type set[E any] interface {
	add(...E)
	contains(E) bool
	members() []E
	equals(set[E]) bool
}

type basicSet[E comparable] map[E]struct{}

func newset[E comparable](vals ...E) set[E] {
	s := basicSet[E]{}
	for _, v := range vals {
		s[v] = struct{}{}
	}
	return s
}

func (s basicSet[E]) equals(other set[E]) bool {
	rhs, ok := other.(basicSet[E])
	if !ok {
		return false
	}

	return maps.Equal(s, rhs)
}

func (s basicSet[E]) add(vals ...E) {
	for _, v := range vals {
		s[v] = struct{}{}
	}
}

func (s basicSet[E]) contains(v E) bool {
	_, ok := s[v]
	return ok
}

func (s basicSet[E]) members() []E {
	result := make([]E, 0, len(s))
	for v := range s {
		result = append(result, v)
	}
	return result
}

type byteSliceSet map[string]struct{}

func newByteSliceSet(vals ...[]byte) byteSliceSet {
	s := byteSliceSet{}
	for _, v := range vals {
		s[string(v)] = struct{}{}
	}
	return s
}

func (bs byteSliceSet) equals(other set[[]byte]) bool {
	rhs, ok := other.(byteSliceSet)
	if !ok {
		return false
	}

	return maps.Equal(bs, rhs)
}

func (bs byteSliceSet) add(vals ...[]byte) {
	for _, v := range vals {
		bs[string(v)] = struct{}{}
	}
}

func (bs byteSliceSet) contains(v []byte) bool {
	_, ok := bs[string(v)]
	return ok
}

func (bs byteSliceSet) members() [][]byte {
	result := make([][]byte, 0, len(bs))
	for v := range bs {
		result = append(result, unsafe.Slice(unsafe.StringData(v), len(v)))
	}
	return result
}
