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

#include "common/macros.h"

namespace peloton {
namespace index {

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

#define SKIPLIST_TYPE SkipList<KeyType, ValueType, KeyComparator, \
        KeyEqualityChecker, ValueEqualityChecker>

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
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
                bool unique_key=false);


};

}  // namespace index
}  // namespace peloton
