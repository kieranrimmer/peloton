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


bool atomicUpdateSkiplistLevel(skip_level_t *src_ptr, const skip_level_t &value) {
  PELOTON_ASSERT(sizeof(void*) == sizeof(int64_t));
  int64_t* cast_src_ptr = reinterpret_cast<int64_t*>((void*)src_ptr);
  int64_t* cast_value_ptr = reinterpret_cast<int64_t*>((void*)&value);
  return __sync_bool_compare_and_swap(cast_src_ptr, *cast_src_ptr,
                                      *cast_value_ptr);
}

bool atomicUpdateSkiplistLevelPtr(void *src_ptr, const void * curVal, const void *newAddr) {
  PELOTON_ASSERT(sizeof(void*) == sizeof(int64_t));
  const int64_t* cast_src_ptr = reinterpret_cast<const int64_t*>(src_ptr);
  const int64_t* curr_addr = reinterpret_cast<const int64_t*>(curVal);
  const int64_t* cast_value_ptr = reinterpret_cast<const int64_t*>(newAddr);
  return __sync_bool_compare_and_swap(cast_src_ptr,
                                      *curr_addr,
                                      *cast_value_ptr);
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Delete(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value) {
  bool retVal = false;
  return retVal;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::AddEntry(UNUSED_ATTRIBUTE ThreadContext &context,
                             UNUSED_ATTRIBUTE const ValueType &value,
                             UNUSED_ATTRIBUTE bool unique_key) {
  return true;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Insert(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value,
                           UNUSED_ATTRIBUTE bool unique_key) {
  // bool retVal = true;

  ThreadContext context{key};
  std::vector<ValueType> value_list;
  GetValue(key, value_list);
  if (unique_key && !value_list.empty())
    return false;




  LOG_TRACE("TRACE LOGGING ENABLED");
  return AddEntry(context, value);
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::AddBottomLevel() {
  void *curTopPtr = topStartNodeAddr;
  BottomNode *nilEndNode = new MaxBottomNode();
  BottomNode *minBotNode = new MinBottomNode(nilEndNode);
  return __sync_bool_compare_and_swap(&topStartNodeAddr, (void*) curTopPtr, (void*) minBotNode);
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::AddLevel() {
  auto *curTopStart = reinterpret_cast<MinNode*>(topStartNodeAddr);
  skip_level_t curTopLevel = curTopStart->getLevel();
  LOG_DEBUG("Adding level %d to skiplist...", curTopLevel + 1);
  UpperNode *nilEndNode = new MaxUpperNode();
  auto *startNode = new MinUpperNode(nilEndNode, curTopStart, reinterpret_cast<MinNode*>(topStartNodeAddr)->getLevel() + 1);
  if (__sync_bool_compare_and_swap(&topStartNodeAddr, (void*) curTopStart, (void*) startNode)) {
    return true;
  }
  delete nilEndNode;
  delete startNode;
  return false;
}

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::SkipList()  {
  LOG_DEBUG("SkipList container class ctor called");
  AddBottomLevel();
  for (int i=1; i < INITIAL_SKIPLIST_NODE_HEIGHT; i++) {
    AddLevel();
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
skip_level_t SKIPLIST_TYPE::getTopLevel() {
  return reinterpret_cast<MinNode*>(topStartNodeAddr)->getLevel();
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::Search(UNUSED_ATTRIBUTE ThreadContext &context, UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) {
  LOG_TRACE("Search()");
//  auto topNode = reinterpret_cast<MinUpperNode*>(topStartNodeAddr);
//  for (auto x: )
  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::GetValue(const KeyType &search_key, UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) {
  LOG_TRACE("GetValue()");

  ThreadContext context{search_key};

  Search(context, value_list);

  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::~SkipList()  {
  LOG_DEBUG("SkipList container class dtor called");
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
