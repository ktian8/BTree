/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * Group member1: name: Xianfu Luo studentID:9082704058
 * Group member2: name: Kexin Tian studentID:9080135420
 * Group member3: name: Haoming Meng studentID:9081272321
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

/**
 * The constructor method for B+tree.
 * We try to first checks if the specified index file exists. Then,
 * concatenating the relational name with the offset of the attribute to
 * construct the index file.
 *
 * @param relationName The relation name of the relation that we want to
 * construct the index
 * @param outIndexName The name of the index file
 * @param bufMgrIn The buffer manager
 * @param attrByteOffset The byte offset of the attribute in the tuple
 * @param attrType The data type of the attribute we are indexing
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // Add your code below. Please do not remove this line.
  std::ostringstream idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  outIndexName = idxStr.str();

  bufMgr = bufMgrIn;
  nodeOccupancy = INTARRAYNONLEAFSIZE;
  scanExecuting = false;
  leafOccupancy = INTARRAYLEAFSIZE;
  pageNum = 0;
  nextEntry = 0;

  try {
    file = new BlobFile(outIndexName, false);
    headerPageNum = file->getFirstPageNo();
    Page *head;
    bufMgrIn->readPage(file, file->getFirstPageNo(), head);
    IndexMetaInfo *meta = (IndexMetaInfo *)head;
    rootPageNum = meta->rootPageNo;
    attributeType = meta->attrType;
    this->attrByteOffset = meta->attrByteOffset;

    if (relationName != meta->relationName ||
        attrByteOffset != meta->attrByteOffset || attrType != meta->attrType) {
      throw BadIndexInfoException(outIndexName);
    }
    bufMgrIn->unPinPage(file, headerPageNum, false);

    FileScan fileScan(relationName, bufMgr);
    RecordId rid;
    try {
      while (1) {
        fileScan.scanNext(rid);
        pageNum++;
      }
    } catch (EndOfFileException &e) {
    }
  } catch (FileNotFoundException &e) {
    file = new BlobFile(outIndexName, true);
    Page *head;
    Page *rootPage;
    IndexMetaInfo *meta;
    bufMgr->allocPage(file, headerPageNum, head);
    bufMgr->allocPage(file, rootPageNum, rootPage);
    initialRootPageNum = rootPageNum;
    meta = (IndexMetaInfo *)head;
    meta->attrType = attrType;
    meta->attrByteOffset = attrByteOffset;
    meta->rootPageNo = rootPageNum;
    strncpy(meta->relationName, relationName.c_str(), 20);
    LeafNodeInt *root = (LeafNodeInt *)rootPage;
    root->rightSibPageNo = 0;
    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);
    FileScan fileScan(relationName, bufMgr);
    RecordId rid;
    try {
      while (1) {
        fileScan.scanNext(rid);
        std::string record = fileScan.getRecord();
        insertEntry(record.c_str() + attrByteOffset, rid);
        pageNum++;
      }
    } catch (EndOfFileException &e) {
      bufMgr->flushFile(file);
    }
    return;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * Destructor that flushes the file, sets scanExecuting to false,
 * deletes the file. Clean up the B+tree.
 */
BTreeIndex::~BTreeIndex() {
  // Add your code below. Please do not remove this line.
  bufMgr->flushFile(BTreeIndex::file);
  scanExecuting = false;
  delete file;
  file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Inserts a tuple into the B+ tree.
 *
 * @param key A pointer to the insert value
 * @param rid The tuple's relation id
 */
void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // Add your code below. Please do not remove this line.
  Page *rootPage;
  bufMgr->readPage(file, rootPageNum, rootPage);
  RIDKeyPair<int> recordInserted;
  recordInserted.set(rid, *((int *)key));
  PageKeyPair<int> *childEntry = nullptr;

  if (initialRootPageNum != rootPageNum) {
    recursiveInsert(rootPage, rootPageNum, false, recordInserted,
                    childEntry);
  } else {
    recursiveInsert(rootPage, rootPageNum, true, recordInserted, childEntry);
  }
}

/**
 * Helper function that inserts the record to the index file recursively
 *
 * @param currentPage Page, The current Page that we are looking at
 * @param currentPageNum PageId, The current Page's page id
 * @param isLeaf bool, True if the current page is a leaf node, false if a
 * nonleaf node
 * @param recordInserted const RIDKeyPair<int>, data entry that will be inserted
 * (RIDKeyPair)
 * @param childEntry PageKeyPair<int>, the new pushed-up entry after
 * splitting. Set to null if no split
 */
void BTreeIndex::recursiveInsert(Page *currentPage, PageId currentPageNum,
                                 bool isLeaf,
                                 const RIDKeyPair<int> recordInserted,
                                 PageKeyPair<int> *&childEntry) {
  // if current node is a leaf node, insert
  if (isLeaf) {
    LeafNodeInt *leafNode = (LeafNodeInt *)currentPage;
    // if the page is full, split the full leaf node
    if (leafNode->ridArray[leafOccupancy - 1].page_number != 0) {
      splitLeafNode(leafNode, currentPageNum, childEntry, entry);
      // the page is not full, insert
    } else {
      insertLeafNode(leafNode, recordInserted);
      childEntry = nullptr;
      bufMgr->unPinPage(file, currentPageNum, true);
    }
    // if current node is a non-leaf node
  } else {
    Page *nextChildPage;
    PageId nextChildNodeNum;
    NonLeafNodeInt *currentNode = (NonLeafNodeInt *)currentPage;
    // if current node's children are leaf nodes
    bool childIsLeaf = (currentNode->level != 0);

    // Find the right child node to look for place to insert
    int ptrIndex = this->nodeOccupancy;
    while (ptrIndex >= 0) {
      if (currentNode->pageNoArray[ptrIndex] != 0) {
        break;
      } else {
        ptrIndex--;
      }
    }
    while (ptrIndex >= 1) {
      if (currentNode->keyArray[ptrIndex - 1] < recordInserted.key) {
        break;
      } else {
        ptrIndex--;
      }
    }
    nextChildNodeNum = currentNode->pageNoArray[ptrIndex];
    bufMgr->readPage(file, nextChildNodeNum, nextChildPage);
    recursiveInsert(nextChildPage, nextChildNodeNum, childIsLeaf,
                    recordInserted, childEntry);

    // if an entry needs to be pushed up and the node needs to be splited
    if (childEntry != nullptr) {
      // if the current page is full
      if (currentNode->pageNoArray[nodeOccupancy] != 0) {
        splitNonLeafNode(currentNode, currentPageNum, childEntry);
      } else {
        // insert the childEntry to current page
        insertNonLeafNode(currentNode, childEntry);
        childEntry = nullptr;
        // finish the insert process, unpin current page
        bufMgr->unPinPage(file, currentPageNum, true);
      }
    } else {
      bufMgr->unPinPage(file, currentPageNum, false);
    }
  }
}

/**
 * Insert a record into the target leaf node
 *
 * @param recordInserted is the record to insert
 * @param leafNode is the target leaf node to insert into
 */
void BTreeIndex::insertLeafNode(LeafNodeInt *leafNode,
                                RIDKeyPair<int> recordInserted) {
  // if non-empty leaf page
  if (leafNode->ridArray[0].page_number != 0) {
    int index = leafOccupancy - 1;
    // find the last record
    while (leafNode->ridArray[index].page_number == 0 && index >= 0) {
      index--;
    }
    // shift record if after the inserted record
    while (leafNode->keyArray[index] > recordInserted.key && index >= 0) {
      leafNode->keyArray[index + 1] = leafNode->keyArray[index];
      leafNode->ridArray[index + 1] = leafNode->ridArray[index];
      index--;
    }
    leafNode->keyArray[index + 1] = recordInserted.key;
    leafNode->ridArray[index + 1] = recordInserted.rid;
  } else {
    leafNode->keyArray[0] = recordInserted.key;
    leafNode->ridArray[0] = recordInserted.rid;
  }
}

/**
 * Insert the record into the corresponding position in the non leaf node
 *
 * @param nonLeafNode is the non leaf node to insert into
 * @param recordInserted is the record to insert
 */
void BTreeIndex::insertNonLeafNode(NonLeafNodeInt *nonLeafNode,
                                   PageKeyPair<int> *recordInserted) {
  int index = nodeOccupancy;
  // find the index of the first occupied record in node
  while (index >= 0) {
    if (nonLeafNode->pageNoArray[index] == 0) {
      index--;
    } else {
      break;
    }
  }
  // find the place to insert
  while (index > 0) {
    if (nonLeafNode->keyArray[index - 1] > recordInserted->key) {
      nonLeafNode->keyArray[index] = nonLeafNode->keyArray[index - 1];
      nonLeafNode->pageNoArray[index + 1] = nonLeafNode->pageNoArray[index];
      index--;
    } else {
      break;
    }
  }
  // insert
  nonLeafNode->keyArray[index] = recordInserted->key;
  nonLeafNode->pageNoArray[index + 1] = recordInserted->pageNo;
}

/**
 * Splits a leaf node when it is full, pushing the middle record up to the
 * parent node; if node is the root, pushed up record becomes the new root
 *
 * @param LeafNode is the full leaf node
 * @param leafPageNum is the page number of the full leaf node
 * @param pagePushedUp is the PageKeyPair that needs to be pushed up
 * @param recordInserted is the data entry that need to be inserted
 */
void BTreeIndex::splitLeafNode(LeafNodeInt *leafNode, PageId leafPageNum,
                               PageKeyPair<int> *&pagePushedUp,
                               const RIDKeyPair<int> recordInserted) {
  PageId newPageNum;
  Page *newPage;

  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  int midIndex =
      (leafOccupancy % 2 == 1) ? (leafOccupancy / 2 + 1) : leafOccupancy / 2;

  // Put half of the records into the new leaf node
  for (int i = 0; i < leafOccupancy - midIndex; ++i) {
    newLeafNode->keyArray[i] = leafNode->keyArray[i + midIndex];
    newLeafNode->ridArray[i] = leafNode->ridArray[i + midIndex];
    // Empty the space after moving
    leafNode->keyArray[i + midIndex] = 0;
    leafNode->ridArray[i + midIndex].page_number = 0;
  }
  // Insert target record
  if (recordInserted.key < leafNode->keyArray[midIndex - 1]) {
    insertLeafNode(leafNode, recordInserted);
  } else {
    insertLeafNode(newLeafNode, recordInserted);
  }
  // Keep sibling pointers up to date
  newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
  leafNode->rightSibPageNo = newPageNum;
  // Use the smallest record from the second page as the pushed up record
  PageKeyPair<int> tempKeyPair;
  tempKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  pagePushedUp = &tempKeyPair;
  if (leafPageNum == rootPageNum) {
    updateRootNode(rootPageNum, pagePushedUp);
  }
  bufMgr->unPinPage(file, newPageNum, true);
  bufMgr->unPinPage(file, leafPageNum, true);
}

/**
 * Splits the full non-leaf node, pushing the middle record up to the parent
 * node, deletes from the the current non leaf level; if the node is the root,
 * make the pushed up record the new root
 *
 * @param origNode is the non-leaf node that needs to be split
 * @param origPageNum is the page id of origNode
 * @param childEntry contains an entry that is pushed up after splitting the
 * node
 */
void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *origNode, PageId origPageNum,
                                  PageKeyPair<int> *&childEntry) {
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  int pushedUpIndex = nodeOccupancy / 2;
  // if even number of keys
  if (nodeOccupancy % 2 == 0) {
    if (childEntry->key >= origNode->keyArray[nodeOccupancy / 2]) {
      pushedUpIndex = nodeOccupancy / 2;
    } else {
      pushedUpIndex = nodeOccupancy / 2 - 1;
    }
  }
  PageKeyPair<int> newChildEntry;
  newChildEntry.set(newPageNum, origNode->keyArray[pushedUpIndex]);
  // Move the larger half of the node to the new node
  for (int i = 0; i < nodeOccupancy - pushedUpIndex - 1; ++i) {
    newNode->keyArray[i] = origNode->keyArray[i + pushedUpIndex + 1];
    newNode->pageNoArray[i] = origNode->pageNoArray[i + pushedUpIndex + 2];
    origNode->keyArray[i + pushedUpIndex + 1] = 0;
    origNode->pageNoArray[i + pushedUpIndex + 2] = (PageId)0;
  }
  // new node and the original node is on the same level
  newNode->level = origNode->level;
  // remove the pushed up record in the non-leaf node
  origNode->keyArray[pushedUpIndex] = 0;
  origNode->pageNoArray[pushedUpIndex] = (PageId)0;
  // insert the pushed up entry
  if (childEntry->key < newNode->keyArray[0]) {
    insertNonLeafNode(origNode, childEntry);
  } else {
    insertNonLeafNode(newNode, childEntry);
  }
  childEntry = &newChildEntry;

  // if the original node is the root node
  if (origPageNum == rootPageNum) {
    updateRootNode(origPageNum, childEntry);
  }
  bufMgr->unPinPage(file, origPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);
}

/**
 * Run when the node to split is the root node, make the pushed up node the new
 * root node
 *
 * @param origRootPageId is the page id of the pointer to the original root page
 * @param childEntry is the entry that is pushed up after splitting
 */
void BTreeIndex::updateRootNode(PageId origRootPageId,
                                PageKeyPair<int> *childEntry) {
  PageId newRootPageNum;
  // Updates tree meta information
  Page *metaPage;
  bufMgr->readPage(file, headerPageNum, metaPage);
  IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)metaPage;

  Page *newRoot;
  bufMgr->allocPage(file, newRootPageNum, newRoot);
  metaInfoPage->rootPageNo = newRootPageNum;

  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;
  // Update new root's information
  newRootPage->pageNoArray[0] = origRootPageId;
  newRootPage->pageNoArray[1] = childEntry->pageNo;
  newRootPage->keyArray[0] = childEntry->key;
  newRootPage->level = (initialRootPageNum != rootPageNum) ? 0 : 1;
  rootPageNum = newRootPageNum;

  // Unpin pages after done
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * This method uses the argument to begin a “filtered scan” of the index
 * and find the right entry for scannext.
 *
 * @param lowValParm is the low value to be tested
 * @param lowOpParm is the operation to be used in testing the low range
 * @param highValParm is the high value to be tested
 * @param highOpParm is the operation to be used in testing the high range
 */
void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm,
                           const void *highValParm, const Operator highOpParm) {
  // Add your code below. Please do not remove this line.
  lowOp = lowOpParm;
  highOp = highOpParm;
  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);
  if (scanExecuting) {
    endScan();
  }
  // check operator
  if (!((lowOp == GT || lowOp == GTE) && (highOp == LT || highOp == LTE))) {
    throw BadOpcodesException();
  }
  if (lowValInt > highValInt) {
    throw BadScanrangeException();
  }
  currentPageNum = rootPageNum;
  bufMgr->readPage(file, currentPageNum, currentPageData);
  //   auto *current = (NonLeafNodeInt *)currentPageData;
  LeafNodeInt *targetNode = nullptr;
  if (pageNum >= leafOccupancy) {  // if the root is not a leaf
    auto *current = (NonLeafNodeInt *)currentPageData;
    bool reachleaf = false;
    while (!reachleaf) {
      if (current->level == 0) {
        int index = 0;
        while (index < nodeOccupancy && current->keyArray[index] <= lowValInt &&
               current->keyArray[index] != 0) {
          index++;
        }
        this->bufMgr->unPinPage(this->file, currentPageNum, false);

        // index now is the one with key that is bigger than low value
        PageId nextPageNo = current->pageNoArray[index];
        currentPageNum = nextPageNo;
        this->bufMgr->readPage(file, currentPageNum, currentPageData);
        current = (NonLeafNodeInt *)currentPageData;
      } else {
        reachleaf = true;
        int index = 0;
        while (index < nodeOccupancy && current->keyArray[index] <= lowValInt &&
               current->keyArray[index] != 0) {
          index++;
        }
        this->bufMgr->unPinPage(this->file, currentPageNum, false);
        PageId nextPageNo = current->pageNoArray[index];
        currentPageNum = nextPageNo;
        this->bufMgr->readPage(file, currentPageNum, currentPageData);
        targetNode = (LeafNodeInt *)currentPageData;
      }
    }

  } else {
    // root is a leaf
    targetNode = (LeafNodeInt *)currentPageData;
  }
  if (targetNode->ridArray[0].page_number == 0) {
    bufMgr->unPinPage(file, currentPageNum, false);
    throw NoSuchKeyFoundException();
  }
  bool found = false;
  while (true) {
    for (int i = 0; i < leafOccupancy; i++) {
      int key = targetNode->keyArray[i];
      if ((highOp == LT && key >= highValInt) ||
          (highOp == LTE && key > highValInt)) {
        // fail to find the key, unpin the page and throw the exception
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
      if (targetNode->keyArray[i] >= lowValInt) {
        // find the entry successfully
        found = true;
        scanExecuting = true;
        break;
      }
    }
    if (found) {
      break;
    }
    if (targetNode->rightSibPageNo == 0) {
      // there is no leaf on the right, fail to find, throw the exception
      this->bufMgr->unPinPage(file, currentPageNum, false);
      throw NoSuchKeyFoundException();
    } else {
      // still not found and there are still leaves on the right, keeping
      // looking
      this->bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = targetNode->rightSibPageNo;
      this->bufMgr->readPage(file, currentPageNum, currentPageData);
      targetNode = (LeafNodeInt *)currentPageData;
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

/**
 * This method fetches the record id of the next tuple that matches the scan
 * criteria
 *
 * @param outRid is the output record id of the next matching entry of the scan
 */
void BTreeIndex::scanNext(RecordId &outRid) {
  // Add your code below. Please do not remove this line.
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  LeafNodeInt *current = (LeafNodeInt *)currentPageData;
  if (current->ridArray[nextEntry].page_number == 0 ||
      nextEntry == this->leafOccupancy - 1) {
    // if we tried all pages in this leaf, go to the right one
    if (current->rightSibPageNo == 0) {
      // if there is no right leaf
      throw IndexScanCompletedException();
    }
    this->bufMgr->unPinPage(this->file, currentPageNum, false);

    // change the current leaf to the right one
    this->currentPageNum = current->rightSibPageNo;
    this->bufMgr->readPage(this->file, this->currentPageNum,
                           this->currentPageData);

    // get the current page
    current = (LeafNodeInt *)this->currentPageData;
    outRid = current->ridArray[0];
    nextEntry = 0;
  }
  int key_index = current->keyArray[nextEntry];
  while ((lowOp == GTE and key_index < lowValInt) or
         (lowOp == GT and key_index <= lowValInt)) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
    key_index = current->keyArray[nextEntry];
  }
  // four cases for the combination of GT,GTE,LT and LTE
  if (lowOp == GT && highOp == LT && key_index > lowValInt &&
      key_index < highValInt) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
  } else if (lowOp == GTE && highOp == LT && key_index >= lowValInt &&
             key_index < highValInt) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
  } else if (lowOp == GT && highOp == LTE && key_index > lowValInt &&
             key_index <= highValInt) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
  } else if (lowOp == GTE && highOp == LTE && key_index >= lowValInt &&
             key_index <= highValInt) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
  } else {
    throw IndexScanCompletedException();
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
  // Add your code below. Please do not remove this line.
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  currentPageData = nullptr;
  bufMgr->unPinPage(file, currentPageNum, false);
  scanExecuting = false;
  currentPageNum = static_cast<PageId>(-1);
  nextEntry = 0;
}

}  // namespace badgerdb
