// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_COMMON_H_
#define BLACKROCK_COMMON_H_

#include <kj/common.h>
#include <kj/io.h>
#include <inttypes.h>

namespace blackrock {

#if __QTCREATOR
#define KJ_MVCAP(var) var
// QtCreator dosen't understand C++14 syntax yet.
#else
#define KJ_MVCAP(var) var = ::kj::mv(var)
// Capture the given variable by move.  Place this in a lambda capture list.  Requires C++14.
//
// TODO(cleanup):  Move to libkj.
#endif

using kj::uint;
using kj::byte;

kj::AutoCloseFd newEventFd(uint value, int flags);
uint64_t readEvent(int fd);
void writeEvent(int fd, uint64_t value);
// TODO(cleanup): Find a better home for these.

// =======================================================================================
// Utility to transform a collection.
//
// TODO(cleanup): Move to KJ.

template <typename T, typename F>
class TransformCollection_ {
public:
  inline constexpr TransformCollection_(T&& inner, F&& func)
      : inner(kj::fwd<T>(inner)), func(kj::fwd<F>(func)) {}

  class Iterator {
  public:
    typedef decltype(kj::instance<T>().begin()) InnerIterator;
    typedef decltype(kj::instance<F>()(*kj::instance<InnerIterator>())) Value;

    Iterator() = default;
    inline Iterator(const F& func, const InnerIterator& inner): func(func), inner(inner) {}

    inline Value operator*() const { return func(*inner); }
    inline Value operator[](size_t index) const { return func(inner[index]); }
    inline Iterator& operator++() { ++inner; return *this; }
    inline Iterator  operator++(int) { return Iterator(func, inner++); }
    inline Iterator& operator--() { --inner; return *this; }
    inline Iterator  operator--(int) { return Iterator(func, inner--); }
    inline Iterator& operator+=(ptrdiff_t amount) { inner += amount; return *this; }
    inline Iterator& operator-=(ptrdiff_t amount) { inner -= amount; return *this; }
    inline Iterator  operator+ (ptrdiff_t amount) const { return Iterator(func, inner + amount); }
    inline Iterator  operator- (ptrdiff_t amount) const { return Iterator(func, inner - amount); }
    inline ptrdiff_t operator- (const Iterator& other) const { return inner - other.inner; }

    inline bool operator==(const Iterator& other) const { return inner == other.inner; }
    inline bool operator!=(const Iterator& other) const { return inner != other.inner; }
    inline bool operator<=(const Iterator& other) const { return inner <= other.inner; }
    inline bool operator>=(const Iterator& other) const { return inner >= other.inner; }
    inline bool operator< (const Iterator& other) const { return inner <  other.inner; }
    inline bool operator> (const Iterator& other) const { return inner >  other.inner; }

  private:
    const F& func;
    InnerIterator inner;
  };

  inline Iterator begin() const { return Iterator(func, inner.begin()); }
  inline Iterator end() const { return Iterator(func, inner.end()); }

  inline auto size() const -> decltype(kj::instance<T>().size()) { return inner.size(); }

private:
  T inner;
  F func;
};

template <typename T, typename F>
constexpr TransformCollection_<T, F> transformCollection(T&& collection, F&& function) {
  // Given a collection and a transformation function, return a new collection whose elements are
  // created by applying the function to each element of the original collection. The new collection
  // is actually implemented by applying the transform when an iterator is dereferenced.
  return TransformCollection_<T, F>(kj::fwd<T>(collection), kj::fwd<F>(function));
}

template <typename From, typename To>
struct StaticCastFunctor {
  inline To operator()(const From& from) const { return static_cast<To>(from); }
};

}  // namespace blackrock

#endif // BLACKROCK_COMMON_H_
