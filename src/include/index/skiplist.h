//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.h
//
// Identification: src/include/index/skiplist.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef _SKIPLIST_H
#define _SKIPLIST_H


#define ALL_PUBLIC

#define INITIAL_SKIPLIST_NODE_HEIGHT 10

#define ASSUMED_CACHE_LINE_SIZE 64

#define MAX_ALLOWED_INSERT_REATTEMPTS 100

#define MIN_NODE_FLAG 0x01
#define NIL_NODE_FLAG 0x03

#define MIN_SKIPLIST_LEVEL 0
#define MAX_SKIPLIST_LEVEL 2000

#include "common/macros.h"


#include <algorithm>
#include <limits>
#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_set>
// offsetof() is defined here
#include <cstddef>
#include <vector>
#include <map>


#include "index/index.h"

#include <inttypes.h>

using SkiplistNodeID = uint64_t;
using flags_t = unsigned char;
using skip_level_t = int;

namespace peloton {
namespace index {

#define SKIPLIST_TEMPLATE_PARAMETERS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

#define SKIPLIST_TYPE \
  SkipList<KeyType, ValueType, KeyComparator, \
        KeyEqualityChecker, ValueEqualityChecker>

#define SKIPLIST_KEY_COMPARATOR_TEMPLATE_PARAMETERS                                       \
  template <typename KeyType, typename KeyComparator>

#define SKIPLIST_KEY_COMPARATOR_TEMPLATE_ARGUMENTS \
  KeyType, KeyComparator

#define SKIPLIST_KEY_EQUALITY_TEMPLATE_PARAMETERS                                       \
  template <typename KeyType, typename KeyEqualityChecker>

#define SKIPLIST_VALUE_EQUALITY_TEMPLATE_PARAMETERS                                       \
  template <typename ValueType, typename ValueEqualityChecker>

// If key1 < key2 return true
SKIPLIST_KEY_COMPARATOR_TEMPLATE_PARAMETERS
inline bool KeyCmpLess(const KeyType &key1, const KeyType &key2) {
  static const KeyComparator keyComparator;
  return keyComparator(key1, key2);
}

SKIPLIST_KEY_COMPARATOR_TEMPLATE_PARAMETERS
inline bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) {
  return !KeyCmpLess<SKIPLIST_KEY_COMPARATOR_TEMPLATE_ARGUMENTS>(key1, key2);
}

SKIPLIST_KEY_COMPARATOR_TEMPLATE_PARAMETERS
inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) {
  return KeyCmpLess<SKIPLIST_KEY_COMPARATOR_TEMPLATE_ARGUMENTS>(key2, key1);
}

SKIPLIST_KEY_COMPARATOR_TEMPLATE_PARAMETERS
inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) {
  return !KeyCmpGreater<SKIPLIST_KEY_COMPARATOR_TEMPLATE_ARGUMENTS>(key1, key2);
}

SKIPLIST_KEY_EQUALITY_TEMPLATE_PARAMETERS
inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) {
  static const KeyEqualityChecker keyEqualityChecker;
  return keyEqualityChecker(key1, key2);
}

SKIPLIST_VALUE_EQUALITY_TEMPLATE_PARAMETERS
inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
  static const ValueEqualityChecker valueEqualityChecker;
  return valueEqualityChecker(v1, v2);
}


SKIPLIST_TEMPLATE_PARAMETERS
class SkipList {

#ifdef ALL_PUBLIC
  public:
#else
  private:
#endif

  const KeyComparator keyComparator;

  const KeyEqualityChecker keyEqualityChecker;

  class ThreadContext;

  class UpperNode;

  class BottomNode;

  class MinNode;

  using SkipListNodeType = BottomNode;

  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list);

  void Search(ThreadContext &context, std::vector<ValueType> &value_list) const;

  UpperNode * TraverseUpperLevel(ThreadContext &context, UpperNode *searchNode) const;

  BottomNode * TraverseBottomLevel(ThreadContext &context, BottomNode *searchNode) const;

  void* AddEntryToUpperLevel(ThreadContext &context, void *downLink, const skip_level_t level);

  void* AddEntryToUpperLevel(ThreadContext &context, const void * downLink);

  KeyType* AddEntryToBottomLevel(ThreadContext &context, ValueType &value);

  UpperNode * InsertKeyIntoUpperLevel(ThreadContext &context, UpperNode *nodeSearched, void *downLink);

  UpperNode * InsertKeyIntoBottomLevel(ThreadContext &context, UpperNode *nodeSearched, void *downLink);

  SkipList<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::MinNode *
  GoToLevel(ThreadContext &context, const skip_level_t level) const;

  static inline bool isNilNode(const flags_t flags) {
    return flags & NIL_NODE_FLAG;
  }

  static inline bool isMinNode(const flags_t flags) {
    return flags & MIN_NODE_FLAG;
  }

  // TODO: Add your declarations here
  public:


  /*
  bool Delete(UNUSED_ATTRIBUTE const KeyType &key,
              UNUSED_ATTRIBUTE const ValueType &value) {
    bool retVal = false;
    return retVal;
  }

  bool Insert(UNUSED_ATTRIBUTE const KeyType &key,
              UNUSED_ATTRIBUTE const ValueType &value,
              UNUSED_ATTRIBUTE bool unique_key=false) {
    bool retVal = false;
    return retVal;
  }
   */

  bool Delete(const KeyType &key,
              const ValueType &value);

  bool Insert(const KeyType &key,
              const ValueType &value,
              bool unique_key = false);

#ifdef ALL_PUBLIC
  public:
#else
  private:
#endif

  bool AddEntry(ThreadContext &context, const KeyType &key, const ValueType &value, bool unique_key);

  void *topStartNodeAddr;

  using KeyNodeIDPair = std::pair<KeyType, SkiplistNodeID>;

  using KeyValuePair = std::pair<KeyType, ValueType>;

  using KeyPointerPair = std::pair<KeyType, void *>;

  bool AddLevel();

  bool AddBottomLevel();

  static const size_t ARR_SIZE = 1;

  static const size_t UPPER_ARR_SIZE = 1;

  public:

  static inline constexpr size_t getArrSize() {
    return ARR_SIZE;
  }


  public:


  class ThreadContext {

#ifdef ALL_PUBLIC
    public:
#else
    private:
#endif
    const KeyType search_key;

    public:
    explicit inline ThreadContext(const KeyType &key)
      : search_key{key} {}

    KeyType getKey() {
      return search_key;
    }

  };


  class UpperNode {
    public:

    KeyType keyArr[UPPER_ARR_SIZE];
    void *downArr[UPPER_ARR_SIZE];
    flags_t flags = 0x00;
    void *forward;

    public:
    inline UpperNode() = delete;

    explicit UpperNode(flags_t _flags) :
        flags{_flags} {}

    explicit inline UpperNode(UpperNode *next, KeyType key, void *down) :
        keyArr{key},
        downArr{down},
        forward{next} {}

    explicit inline UpperNode(UpperNode *next, KeyType key, flags_t _flags, void *down) :
        keyArr{key},
        downArr{down},
        flags{_flags},
        forward{next} {}

    // for min and max keys
    explicit inline UpperNode(UpperNode *next, flags_t _flags, void *down) :
        downArr{down},
        flags{_flags},
        forward{next} {}

    inline bool containsGreaterThanEqualKey(const KeyType searchKey) const;

    UpperNode *getForward() {
      return (UpperNode *) forward;
    }

  };

  class MinNode {

#ifdef ALL_PUBLIC
    public:
#else
    private:
#endif
    skip_level_t level = -1;

    public:

    MinNode() = delete;

    explicit inline MinNode(skip_level_t _level)
      : level(_level) {
      level = _level;
    }

    inline void setLevel(skip_level_t _level) {
      level = _level;
    }

    inline skip_level_t getLevel() const {
      return level;
    }

  };


  class MinUpperNode : public MinNode, public UpperNode {

    public:

    MinUpperNode() = delete;

    explicit MinUpperNode(UpperNode *next, void *down, skip_level_t _level) :
        MinNode(_level),
        UpperNode(next, MIN_NODE_FLAG, down)
        {}

  };

  class MaxUpperNode : public UpperNode {

    public:

    MaxUpperNode() :
        UpperNode(NIL_NODE_FLAG) {}

    explicit MaxUpperNode(void *down) :
        UpperNode(nullptr, NIL_NODE_FLAG, down) {}

  };

  class BottomNode {
    public:
    KeyType keyArr[ARR_SIZE];
    ValueType valArr[ARR_SIZE];
    flags_t flags = 0x00;
    void *forward;
    public:
    inline BottomNode() : forward() {}

    explicit inline BottomNode(BottomNode *next, KeyType key, ValueType val) :
        keyArr{key},
        valArr{val},
        forward{next} {}

    explicit inline BottomNode(BottomNode *next, KeyType key, flags_t _flags, ValueType val) :
        keyArr{key},
        valArr{val},
        flags{_flags},
        forward{next} {}

    // for min and max keys
    explicit inline BottomNode(BottomNode *next, flags_t _flags) :
        flags{_flags},
        forward{next} {}

    BottomNode *getForward() {
      return (BottomNode *) forward;
    }

    inline bool containsGreaterThanEqualKey(const KeyType searchKey) const;

  };

  class MinBottomNode : public MinNode,  public BottomNode {
    public:
    MinBottomNode() = delete;

    explicit inline MinBottomNode(BottomNode *next)
      : MinNode(0),
        BottomNode(next, MIN_NODE_FLAG) {

    }
  };

  class MaxBottomNode : public BottomNode {
    public:
    inline MaxBottomNode() :
        BottomNode(nullptr, NIL_NODE_FLAG) {}
  };

  public:

  SkipList();

  ~SkipList();

  skip_level_t getTopLevel();


};

}  // namespace index
}  // namespace peloton


#endif // _SKIPLIST_H
