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

#include <include/index/skiplist.h>
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
  int64_t* cast_src_ptr = reinterpret_cast<int64_t*>(src_ptr);
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
  auto entry = AddEntryToBottomLevel(context, value);
  // LOG_DEBUG("Adding BottomNode which should NOT be deletable, Node details: level = %d, flags = %d, address = %p", 0, entry->flags, (void*) entry);
  int i=1;
  auto downP = entry;
  while (i < INITIAL_SKIPLIST_NODE_HEIGHT) {
    downP = AddEntryToUpperLevel(context, downP, (skip_level_t) i);
    // LOG_DEBUG("Adding SkipListNode which should NOT be deletable, Node details: level = %d, flags = %d, address = %p", i, ((SkipListNode*) downP)->flags, downP);
    ++i;
  }
  auto upperNode = downP;
  upperNode->MakeDeletable();
  PELOTON_ASSERT(upperNode->IsDeletable());
  while (!upperNode->IsBottomNode()) {
    upperNode = upperNode->GetDown(key);
    upperNode->MakeDeletable();
    // LOG_DEBUG("SkipListNode should be deletable, Node details: level = %d, flags = %d, address = %p", i, upperNode->flags, (void*) upperNode);
    PELOTON_ASSERT(upperNode->IsDeletable());
    --i;
  }
  auto bottomNode = upperNode;
  bottomNode->MakeDeletable();
  PELOTON_ASSERT(bottomNode->IsDeletable());
  return entry != nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::Insert(UNUSED_ATTRIBUTE const KeyType &key,
                           UNUSED_ATTRIBUTE const ValueType &value,
                           UNUSED_ATTRIBUTE bool unique_key) {

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
  auto curTopPtr = topStartNode.load();
  auto nilEndNode = createMaxBottomNode();
  auto minBotNode = createMinBottomNode(nilEndNode);
  return topStartNode.compare_exchange_weak(curTopPtr, minBotNode);
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::AddLevel() {
  auto curTopStart = topStartNode.load();
  auto nilEndNode = createMaxUpperNode();
  auto startNode = createMinUpperNode(nilEndNode, curTopStart, curTopStart->GetLevel() + 1);
  if (topStartNode.compare_exchange_weak(curTopStart, startNode)) {
    return true;
  }
  delete nilEndNode;
  delete startNode;
  return false;
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::SkipListNode::ContainsGreaterThanEqualKey(const KeyType searchKey) const {
  return ScanKey(searchKey) < (arraySize - 1);
}

SKIPLIST_TEMPLATE_PARAMETERS
bool SKIPLIST_TYPE::BottomSkipListNode::ContainsGreaterThanEqualKey(const KeyType searchKey) const {
  return ScanKey(searchKey) < (arraySize - 1);
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::BaseSkipListNode * SKIPLIST_TYPE::TraverseLevel(ThreadContext &context, BaseSkipListNode *searchNode) const {
  auto nextNode = searchNode->GetNext();
  while (!searchNode->IsNilNode() && nextNode->ContainsGreaterThanEqualKey(context.getKey())) {
    searchNode = nextNode;
    nextNode = searchNode->GetNext();
  }
  return searchNode;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::MinSkipListNode* SKIPLIST_TYPE::GoToLevel(UNUSED_ATTRIBUTE ThreadContext &context, const skip_level_t level) const {
  PELOTON_ASSERT(level >= MIN_SKIPLIST_LEVEL && level <= std::numeric_limits<skip_level_t>::max());
  auto searchNode = topStartNode.load();
  PELOTON_ASSERT(searchNode->GetLevel() >= level);
  while (searchNode->GetLevel() >= MIN_SKIPLIST_LEVEL) {
    if (searchNode->GetLevel() == level)
      return searchNode;
    auto upperNode = searchNode;
    searchNode = upperNode->GoToLevelBelow();
  }
  PELOTON_ASSERT(false);
  return nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::BaseSkipListNode * SKIPLIST_TYPE::AddEntryToBottomLevel(ThreadContext &context, UNUSED_ATTRIBUTE const ValueType &value,
                                                                            BaseSkipListNode *startingPoint) {
  if (startingPoint == nullptr)
    startingPoint = static_cast<MinSkipListNode*>(GoToLevel(context, 0));
  auto nodeSearched = TraverseLevel(context, startingPoint);
  auto nodeToInsert = new BottomSkipListNode{
    nodeSearched->GetNext()
  };
  int attempts = 0;
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, nodeSearched->forward, nodeToInsert)) {
      return nodeToInsert;
    }
    if (attempts++ > MAX_ALLOWED_INSERT_REATTEMPTS) {
      delete(nodeToInsert);
      break;
    }
    nodeSearched = TraverseLevel(context, static_cast<MinSkipListNode*>(GoToLevel(context, 0)));
    nodeToInsert->forward = nodeSearched->forward;
  }
  return nodeToInsert;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::BaseSkipListNode* SKIPLIST_TYPE::InsertKeyIntoUpperLevel(UNUSED_ATTRIBUTE ThreadContext &context, BaseSkipListNode *nodeSearched, BaseSkipListNode *downLink) {
  auto nodeToInsert = new SkipListNode{
    nodeSearched->GetNext(),
    downLink,
    context.getKey()
  };
  // PELOTON_ASSERT(!(nodeToInsert->IsDeletable()));
  int attempts = 0;
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, nodeSearched->forward, nodeToInsert)) {
      return nodeToInsert;
    }
    if (attempts++ > MAX_ALLOWED_INSERT_REATTEMPTS) {
      delete(nodeToInsert);
      break;
    }
  }
  return nullptr;
}

SKIPLIST_TEMPLATE_PARAMETERS
typename SKIPLIST_TYPE::BaseSkipListNode* SKIPLIST_TYPE::InsertKeyIntoBottomLevel(UNUSED_ATTRIBUTE ThreadContext &context, BaseSkipListNode *nodeSearched) {
  auto nodeToInsert = new BottomSkipListNode{
    nodeSearched->GetNext()
  };
  int attempts = 0;
  while (true) {
    if (__sync_bool_compare_and_swap(&nodeSearched->forward, nodeSearched->forward, nodeToInsert)) {
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
typename SKIPLIST_TYPE::BaseSkipListNode *SKIPLIST_TYPE::AddEntryToUpperLevel(UNUSED_ATTRIBUTE ThreadContext &context, BaseSkipListNode *downLink, const skip_level_t level) {
  PELOTON_ASSERT(level > MIN_SKIPLIST_LEVEL && level <= std::numeric_limits<skip_level_t>::max());
  auto upperStartNode = GoToLevel(context, level);
  auto searchNode = TraverseLevel(context, upperStartNode);
  return InsertKeyIntoUpperLevel(context, searchNode, downLink);
}

SKIPLIST_TEMPLATE_PARAMETERS
SKIPLIST_TYPE::SkipList():
    keyComparator{KeyComparator{}},
    keyEqualityChecker{KeyEqualityChecker{}} {
  LOG_DEBUG("SkipList container class ctor called");
  PELOTON_ASSERT(AddBottomLevel());
  for (int i=1; i < INITIAL_SKIPLIST_NODE_HEIGHT; i++) {
    AddLevel();
  }
}

SKIPLIST_TEMPLATE_PARAMETERS
skip_level_t SKIPLIST_TYPE::getTopLevel() {
  return topStartNode.load()->GetLevel();
}

SKIPLIST_TEMPLATE_PARAMETERS
void SKIPLIST_TYPE::Search(UNUSED_ATTRIBUTE ThreadContext &context, UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) const {
  LOG_TRACE("Search()");

  auto topNode = topStartNode.load();
  while (topNode->GetLevel() > 1) {
    LOG_DEBUG("iterating down SkipList, currently on level %d", topNode->GetLevel());
    topNode = GoToLevel(context, topNode->GetLevel() - 1);
  }
  UNUSED_ATTRIBUTE auto bottomNode = GoToLevel(context, 0);
  auto node = TraverseLevel(context, bottomNode);
  if (auto datum = node->GetData(context.getKey()))
    value_list.push_back(datum);
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
