//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.cpp
//
// Identification: src/index/skiplist.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/skiplist.h"

#include "index/index_key.h"

namespace peloton {
namespace index {

// Add your function definitions here

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Delete(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value) {
  bool retVal = false;
  return retVal;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Insert(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value,
                           UNUSED_ATTRIBUTE bool unique_key) {
  bool retVal = false;
  return retVal;
}


        template class SkipList<
                CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
        CompactIntsEqualityChecker<1>, ItemPointerComparator>;
        template class SkipList<
                CompactIntsKey<2>, ItemPointer *, CompactIntsComparator<2>,
        CompactIntsEqualityChecker<2>, ItemPointerComparator>;
        template class SkipList<
                CompactIntsKey<3>, ItemPointer *, CompactIntsComparator<3>,
        CompactIntsEqualityChecker<3>, ItemPointerComparator>;
        template class SkipList<
                CompactIntsKey<4>, ItemPointer *, CompactIntsComparator<4>,
        CompactIntsEqualityChecker<4>, ItemPointerComparator>;

// Generic key
        template class SkipList<GenericKey<4>, ItemPointer *,
        FastGenericComparator<4>,
        GenericEqualityChecker<4>, ItemPointerComparator>;
        template class SkipList<GenericKey<8>, ItemPointer *,
        FastGenericComparator<8>,
        GenericEqualityChecker<8>, ItemPointerComparator>;
        template class SkipList<GenericKey<16>, ItemPointer *,
        FastGenericComparator<16>,
        GenericEqualityChecker<16>, ItemPointerComparator>;
        template class SkipList<GenericKey<64>, ItemPointer *,
        FastGenericComparator<64>,
        GenericEqualityChecker<64>, ItemPointerComparator>;
        template class SkipList<
                GenericKey<256>, ItemPointer *, FastGenericComparator<256>,
        GenericEqualityChecker<256>, ItemPointerComparator>;

// Tuple key
        template class SkipList<TupleKey, ItemPointer *, TupleKeyComparator,
                TupleKeyEqualityChecker, ItemPointerComparator>;

}  // namespace index
}  // namespace peloton
