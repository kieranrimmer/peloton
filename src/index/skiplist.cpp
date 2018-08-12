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

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::Delete(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value) {
  bool retVal = false;
  return retVal;
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::AddEntry(UNUSED_ATTRIBUTE ThreadContext &context,
                             UNUSED_ATTRIBUTE const KeyType &key,
                             UNUSED_ATTRIBUTE const ValueType &value,
                             UNUSED_ATTRIBUTE bool unique_key) {
  // skip_level_t curLevel(0);
  // AddEntryToUpperLevel()
  // while (curLevel < MAX_SKIPLIST_LEVEL) {
    // AddEntryToUpperLevel()
  // }
  return true;
}

SKIPLIST_TEMPLATE_PARAMETERS
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
  return AddEntry(context, key, value, false);
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::AddBottomLevel() {
  void *curTopPtr = topStartNodeAddr;
  BottomNode *nilEndNode = new MaxBottomNode();
  BottomNode *minBotNode = new MinBottomNode(nilEndNode);
  return __sync_bool_compare_and_swap(&topStartNodeAddr, (void*) curTopPtr, (void*) minBotNode);
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::AddLevel() {
  auto *curTopStart = reinterpret_cast<MinNode *>(topStartNodeAddr);
  skip_level_t curTopLevel = curTopStart->getLevel();
  LOG_DEBUG("Adding level %d to skiplist...", curTopLevel + 1);
  UpperNode *nilEndNode = new MaxUpperNode();
  auto *startNode = new MinUpperNode(nilEndNode, curTopStart, curTopStart->getLevel() + 1);
  if (__sync_bool_compare_and_swap(&topStartNodeAddr, (void*) curTopStart, (void*) startNode)) {
    return true;
  }
  delete nilEndNode;
  delete startNode;
  return false;
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::UpperNode::containsGreaterThanEqualKey(const KeyType searchKey) const {
  for (int i=0; (size_t) i < UPPER_ARR_SIZE; ++i) {
    if (KeyCmpGreaterEqual<KeyType, KeyComparator>(searchKey, keyArr[i]))
      return true;
  }
  return false;
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::BottomNode::containsGreaterThanEqualKey(const KeyType searchKey) const {
  for (int i=0; (size_t) i < UPPER_ARR_SIZE; ++i) {
    if (KeyCmpGreaterEqual<KeyType, KeyComparator>(searchKey, keyArr[i]))
      return true;
  }
  return false;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::UpperNode * SKIPLIST_TYPE::TraverseUpperLevel(ThreadContext &context, UpperNode *searchNode) const {
  PELOTON_ASSERT(isMinNode(searchNode->flags));
  UpperNode *nextNode = searchNode->getForward();
  do {
    searchNode = nextNode;
    nextNode = searchNode->getForward();
  } while (!isNilNode(searchNode->flags) && nextNode->containsGreaterThanEqualKey(context.getKey()));
  return searchNode;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::BottomNode * SKIPLIST_TYPE::TraverseBottomLevel(ThreadContext &context, BottomNode *searchNode) const {
  PELOTON_ASSERT(isMinNode(searchNode->flags));
  auto nextNode = reinterpret_cast<BottomNode*>(searchNode->getForward());
  do {
    searchNode = nextNode;
    nextNode = searchNode->getForward();
  } while (!isNilNode(searchNode->flags) && nextNode->containsGreaterThanEqualKey(context.getKey()));
  return searchNode;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::MinNode* SKIPLIST_TYPE::GoToLevel(UNUSED_ATTRIBUTE ThreadContext &context, const skip_level_t level) const {
  PELOTON_ASSERT(level >= MIN_SKIPLIST_LEVEL && level <= std::numeric_limits<skip_level_t>::max());
  auto searchNode = reinterpret_cast<MinNode*>(topStartNodeAddr);
  PELOTON_ASSERT(searchNode->getLevel() <= level);
  while (searchNode->getLevel() >= MIN_SKIPLIST_LEVEL) {
    if (searchNode->getLevel() == level)
      return searchNode;
  }
  PELOTON_ASSERT(false);
  return nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
KeyType* SKIPLIST_TYPE::AddEntryToBottomLevel(ThreadContext &context, ValueType &value) {
  auto nodeToInsert = new BottomNode{
    nullptr,
    context.getKey(),
    value
  };
  int attempts = 0;
  auto nodeSearched = TraverseBottomLevel(context, reinterpret_cast<MinBottomNode*>(GoToLevel(context, 0)));
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, (void*) nodeSearched->forward, (void*) nodeToInsert)) {
      return &nodeToInsert->keyArr[0];
    }
    if (attempts++ > MAX_ALLOWED_INSERT_REATTEMPTS) {
      delete(nodeToInsert);
      break;
    }
  }
  return nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::UpperNode* SKIPLIST_TYPE::InsertKeyIntoUpperLevel(UNUSED_ATTRIBUTE ThreadContext &context, UpperNode *nodeSearched, void *downLink) {
  auto nodeToInsert = new UpperNode{
    (UpperNode *) nodeSearched->forward,
    context.getKey(),
    downLink
  };
  int attempts = 0;
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, (void*) nodeSearched->forward, (void*) nodeToInsert)) {
      return nodeToInsert;
    }
    if (attempts++ > MAX_ALLOWED_INSERT_REATTEMPTS) {
      delete(nodeToInsert);
      break;
    }
  }
  // PELOTON_ASSERT(false);
  return nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::UpperNode* SKIPLIST_TYPE::InsertKeyIntoBottomLevel(UNUSED_ATTRIBUTE ThreadContext &context, UpperNode *nodeSearched, void *downLink) {
  auto nodeToInsert = new UpperNode{
    (UpperNode *) nodeSearched->forward,
    context.getKey(),
    downLink
  };
  int attempts = 0;
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, (void*) nodeSearched->forward, (void*) nodeToInsert)) {
      return nodeToInsert;
    }
    if (attempts++ > MAX_ALLOWED_INSERT_REATTEMPTS) {
      delete(nodeToInsert);
      break;
    }
  }
  // PELOTON_ASSERT(false);
  return nullptr;
}


SKIPLIST_TEMPLATE_PARAMETERS
void* SKIPLIST_TYPE::AddEntryToUpperLevel(UNUSED_ATTRIBUTE ThreadContext &context, void *downLink, const skip_level_t level) {
  PELOTON_ASSERT(level > MIN_SKIPLIST_LEVEL && level <= std::numeric_limits<skip_level_t>::max());
  auto upperStartNode = reinterpret_cast<MinUpperNode*>(GoToLevel(context, level));
  auto searchNode = TraverseUpperLevel(context, upperStartNode);
  return InsertKeyIntoUpperLevel(context, searchNode, downLink);
}

SKIPLIST_TEMPLATE_PARAMETERS
SKIPLIST_TYPE::SkipList():
    keyComparator{KeyComparator{}},
    keyEqualityChecker{KeyEqualityChecker{}} {
  LOG_DEBUG("SkipList container class ctor called");
  AddBottomLevel();
  for (int i=1; i < INITIAL_SKIPLIST_NODE_HEIGHT; i++) {
    AddLevel();
  }
}

SKIPLIST_TEMPLATE_PARAMETERS
skip_level_t SKIPLIST_TYPE::getTopLevel() {
  return reinterpret_cast<MinNode*>(topStartNodeAddr)->getLevel();
}

SKIPLIST_TEMPLATE_PARAMETERS
void SKIPLIST_TYPE::Search(UNUSED_ATTRIBUTE ThreadContext &context, UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) const {
  LOG_TRACE("Search()");
//  auto topNode = reinterpret_cast<MinUpperNode*>(topStartNodeAddr);
//  for (auto x: )
  return;
}

SKIPLIST_TEMPLATE_PARAMETERS
void SKIPLIST_TYPE::GetValue(const KeyType &search_key, UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) {
  LOG_TRACE("GetValue()");

  ThreadContext context{search_key};

  Search(context, value_list);

  return;
}

SKIPLIST_TEMPLATE_PARAMETERS
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
