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

#ifndef ALL_PUBLIC
#define ALL_PUBLIC
#endif

#define INITIAL_SKIPLIST_NODE_HEIGHT 10

#define ASSUMED_CACHE_LINE_SIZE 64

#define MAX_ALLOWED_INSERT_REATTEMPTS 100

#define MIN_NODE_FLAG 0x01
#define NIL_NODE_FLAG 0x02
#define IS_DELETABLE_FLAG 0x04
#define IS_DELETED_FLAG 0x08
#define IS_BOTTOM_FLAG 0x0f

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

#include "common/synchronization/readwrite_latch.h"
#include "index/index.h"

#include <inttypes.h>

using SkiplistNodeID = uint64_t;
using flags_t = unsigned char;
using skip_level_t = int;

namespace peloton {
namespace index {

using ::peloton::common::synchronization::ReadWriteLatch;

class ReadLatchRAII {
#ifdef ALL_PUBLIC
  public:
#else
  private:
#endif
  ReadWriteLatch &_ReadWriteLatch;
  public:
  explicit ReadLatchRAII(ReadWriteLatch &_RWLatch): _ReadWriteLatch(_RWLatch) {
    _ReadWriteLatch.ReadLock();
  }

//  explicit ReadLatchRAII(const ReadLatchRAII&) = delete;
  ReadLatchRAII &operator=(const ReadLatchRAII&) = delete;

  ~ReadLatchRAII() {
    _ReadWriteLatch.Unlock();
  }

};

class WriteLatchRAII {
#ifdef ALL_PUBLIC
  public:
#else
  private:
#endif
  ReadWriteLatch &_ReadWriteLatch;
  public:
  WriteLatchRAII(ReadWriteLatch &_RWLatch): _ReadWriteLatch(_RWLatch) {
    _ReadWriteLatch.WriteLock();
  }

  WriteLatchRAII(const WriteLatchRAII&) = delete;
  WriteLatchRAII &operator=(const WriteLatchRAII&) = delete;

  ~WriteLatchRAII() {
    _ReadWriteLatch.Unlock();
  }

};

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

  static_assert(sizeof(ValueType) == sizeof(void*),
    "SkipList depends upon native pointer type as ValueType");

#ifdef ALL_PUBLIC
  public:
#else
  private:
#endif

  const KeyComparator keyComparator;

  const KeyEqualityChecker keyEqualityChecker;

  class ThreadContext;

  class BaseSkipListNode;

  class SkipListNode;

  class MinSkipListNode;

  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list);

  void Search(ThreadContext &context, std::vector<ValueType> &value_list) const;

  BaseSkipListNode * TraverseLevel(ThreadContext &context, BaseSkipListNode *searchNode) const;

  bool AddEntry(ThreadContext &context, const KeyType &key, const ValueType &value, bool unique_key);

  BaseSkipListNode *AddEntryToUpperLevel(ThreadContext &context, BaseSkipListNode *downLink, const skip_level_t level);

  BaseSkipListNode * AddEntryToBottomLevel(ThreadContext &context, const ValueType &value,
                                       BaseSkipListNode *startingPoint = nullptr);

  BaseSkipListNode * InsertKeyIntoUpperLevel(ThreadContext &context, BaseSkipListNode *nodeSearched, BaseSkipListNode *downLink);

  BaseSkipListNode * InsertKeyIntoBottomLevel(ThreadContext &context, BaseSkipListNode *nodeSearched);

  SkipList<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::MinSkipListNode *
  GoToLevel(ThreadContext &context, const skip_level_t level) const;

  static inline bool isNilNode(const flags_t flags) {
    return flags & (flags_t) NIL_NODE_FLAG;
  }

  static inline bool isMinNode(const flags_t flags) {
    return flags & (flags_t) MIN_NODE_FLAG;
  }

  static inline void makeDeletable(flags_t &flags) {
    flags ^= IS_DELETABLE_FLAG;
  }

  static inline void makeDeleted(flags_t &flags) {
    flags ^= IS_DELETED_FLAG;
  }

  static inline bool isDeletable(const flags_t flags) {
    return flags & (flags_t) IS_DELETABLE_FLAG;
  }

  static inline bool isDeleted(const flags_t flags) {
    return flags & (flags_t) IS_DELETED_FLAG;
  }

  static inline bool isBottomNode(const flags_t flags) {
    return flags & (flags_t) IS_BOTTOM_FLAG;
  }

  // TODO: Add your declarations here
  public:


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



  std::atomic<MinSkipListNode*> topStartNode;

  using KeyNodeIDPair = std::pair<KeyType, SkiplistNodeID>;

  using KeyValuePair = std::pair<KeyType, ValueType>;

  using KeyPointerPair = std::pair<KeyType, void *>;

  bool AddLevel();

  bool AddBottomLevel();

  static const int ARR_SIZE = 1;

  static const int UPPER_ARR_SIZE = 1;

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

  class BaseSkipListNode {

#ifdef ALL_PUBLIC
    public:
#else
    private:
#endif
    BaseSkipListNode *forward;

    public:
    explicit BaseSkipListNode(BaseSkipListNode *_forward): forward{_forward} {}

    BaseSkipListNode *GetNext() const {
      return forward;
    }

    virtual bool IsFull() const {
      return false;
    }

    virtual bool IsBottomNode() const {
      return false;
    }

    virtual bool IsNilNode() const {
      return false;
    }

    virtual bool ContainsGreaterThanEqualKey(const UNUSED_ATTRIBUTE KeyType searchKey) const = 0;

    virtual int ScanKey(const KeyType key, bool hasWriteLock = false) const = 0;
    virtual ValueType GetData(const KeyType key) const = 0;
    virtual BaseSkipListNode *GetDown(const KeyType key) const = 0;
    virtual bool MakeUndeletable() = 0;
    virtual bool MakeDeletable() = 0;
    virtual bool IsDeletable() const = 0;


    bool SetForward(BaseSkipListNode *_forward);

    virtual bool AddEntry(const UNUSED_ATTRIBUTE KeyValuePair keyVal) {
      return false;
    }

    virtual ReadWriteLatch *GetLatch() {
      return nullptr;
    }



  };

  class NilSkipListNode: public BaseSkipListNode {

    public:
    bool IsNilNode() const override {
      return true;
    }

    bool ContainsGreaterThanEqualKey(const UNUSED_ATTRIBUTE KeyType searchKey) const override {
      return true;
    }

    ValueType GetData(UNUSED_ATTRIBUTE const KeyType key) const override {
      return nullptr;
    }

    BaseSkipListNode *GetDown(UNUSED_ATTRIBUTE const KeyType key) const override {
      return nullptr;
    }

    bool MakeUndeletable() override {
      return true;
    }

    bool MakeDeletable() override {
      return true;
    }

    bool IsDeletable() const override {
      return true;
    }

  };

  class MinSkipListNode: public BaseSkipListNode {
#ifdef ALL_PUBLIC
    public:
#else
    private:
#endif
    MinSkipListNode *down;
    skip_level_t level;
    flags_t flags;

    public:

    MinSkipListNode() = delete;

    explicit MinSkipListNode(BaseSkipListNode *forward, MinSkipListNode *_down, skip_level_t _level, flags_t _flags)
      : BaseSkipListNode{forward}, down{_down}, level{_level}, flags{_flags} {}

    skip_level_t GetLevel() const {
      return level;
    }

    bool IsBottomNode() const override {
      return level == 0;
    }

    BaseSkipListNode *GetDown(UNUSED_ATTRIBUTE const KeyType key) const override {
      return down;
    }

    MinSkipListNode *GoToLevelBelow() const {
      return down;
    }

    int ScanKey(const UNUSED_ATTRIBUTE KeyType key, UNUSED_ATTRIBUTE bool hasWriteLock = false) const override {
      return -1;
    }

    ValueType GetData(UNUSED_ATTRIBUTE const KeyType key) const override {
      return nullptr;
    }

    bool ContainsGreaterThanEqualKey(const UNUSED_ATTRIBUTE KeyType searchKey) const override {
      return false;
    }

    bool MakeUndeletable() override {
      return true;
    }

    bool MakeDeletable() override {
      return true;
    }

    bool IsDeletable() const override {
      return true;
    }

  };



  class SkipListNode: public BaseSkipListNode {

#ifdef ALL_PUBLIC
    public:
#else
    private:
#endif
    struct internal_node_payload  {
      KeyType keyArr[ARR_SIZE];
      BaseSkipListNode *downArr[ARR_SIZE];
    } payload;

    int arraySize;

    mutable ReadWriteLatch _ReadWriteLatch;

    flags_t flags = 0x00;


    public:
    inline SkipListNode() = delete;

    explicit SkipListNode(flags_t _flags) :
        BaseSkipListNode{nullptr},
        arraySize{0},
        _ReadWriteLatch{},
        flags{_flags} {}

    inline SkipListNode(BaseSkipListNode *next, BaseSkipListNode *down, KeyType key) :
        BaseSkipListNode{next},
        _ReadWriteLatch{}
         {
           payload.keyArr[0] = key;
           payload.downArr[0] = down;
         }

    // for min and max keys
    inline SkipListNode(SkipListNode *next, flags_t _flags) :
      BaseSkipListNode{next},
      _ReadWriteLatch{},
        flags{_flags}
         {}


    int ScanKey(const KeyType key, bool hasWriteLock = false) const override {
      if (!hasWriteLock)
        auto readLatch = ReadLatchRAII{_ReadWriteLatch};
      int i = -1;
      while (i < ARR_SIZE - 1) {
        if(KeyCmpGreater<KeyType, KeyComparator>(payload.keyArr[i + 1], key))
          break;
        ++i;
      }
      return i;
    }

    bool ContainsGreaterThanEqualKey(const KeyType searchKey) const override;


    void InsertKey(KeyValuePair );



    BaseSkipListNode *GetDown(const KeyType key) const override {
      if (auto i = ScanKey(key) > -1)
        return payload.downArr[i];
      return nullptr;
    }

    ValueType GetData(UNUSED_ATTRIBUTE const KeyType key) const override {
      return nullptr;
    }

    ReadWriteLatch *GetLatch() override {
      return &_ReadWriteLatch;
    }

    bool MakeUndeletable() override {
      return true;
    }

    bool MakeDeletable() override {
      return true;
    }

    bool IsDeletable() const override {
      return true;
    }

  };

  class BottomSkipListNode: public BaseSkipListNode {

    struct bottom_node_payload {
      KeyType keyArr[ARR_SIZE];
      ValueType downArr[ARR_SIZE];
    } payload;

    int arraySize;

    ReadWriteLatch _ReadWriteLatch;

    public:


    flags_t flags = 0x00;

    public:
    inline BottomSkipListNode() = delete;

    explicit BottomSkipListNode(flags_t _flags) :
      BaseSkipListNode{nullptr},
      flags{_flags} {}

    explicit inline BottomSkipListNode(BaseSkipListNode *next) :
      BaseSkipListNode{next}
    {}

    // for min and max keys
    inline BottomSkipListNode(BaseSkipListNode *next, flags_t _flags) :
      BaseSkipListNode{next},
      flags{_flags}
    {}

    bool ContainsGreaterThanEqualKey(const KeyType searchKey) const override;

    BaseSkipListNode *GetDown(UNUSED_ATTRIBUTE const KeyType key) const override {
      return nullptr;
    }

    int ScanKey(const KeyType key, UNUSED_ATTRIBUTE bool hasWriteLock = false) const override {
      int i = -1;
      while ((i < ARR_SIZE - 1) && KeyCmpLess<KeyType, KeyComparator>(payload.keyArr[i + 1], key)) {
        ++i;
      }
      return i;
    }

    ValueType GetData(const KeyType key) const override {
      int i = ScanKey(key);
      return i > -1 && KeyCmpEqual<KeyType, KeyComparator>(payload.keyArr[i], key) ? payload.downArr[i] : nullptr;
    }

    ReadWriteLatch *GetLatch() override {
      return &_ReadWriteLatch;
    }

    bool MakeUndeletable() override {
      return true;
    }

    bool MakeDeletable() override {
      return true;
    }

    bool IsDeletable() const override {
      return true;
    }

  };

  BaseSkipListNode *createMaxUpperNode() {
    return new SkipListNode((flags_t) NIL_NODE_FLAG);
  }

  BaseSkipListNode *createMaxBottomNode() {
    return new SkipListNode((flags_t) NIL_NODE_FLAG & (flags_t) IS_BOTTOM_FLAG);
  }

  MinSkipListNode *createMinUpperNode(BaseSkipListNode *next, MinSkipListNode *down, skip_level_t level) {
    return new MinSkipListNode(next, down, level, MIN_NODE_FLAG);
  }

  MinSkipListNode *createMinBottomNode(BaseSkipListNode *next) {
    return new MinSkipListNode(next, nullptr, 0, (flags_t) MIN_NODE_FLAG & (flags_t) IS_BOTTOM_FLAG);
  }

  public:

  SkipList();

  ~SkipList();

  skip_level_t getTopLevel();


};

}  // namespace index
}  // namespace peloton


#endif // _SKIPLIST_H
