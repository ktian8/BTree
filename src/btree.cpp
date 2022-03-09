/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include <climits>
#include <vector>

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

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  std::ostringstream idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  std::string indexName = idxStr.str();
  outIndexName = indexName;

  this->bufMgr = bufMgrIn;
  this->attributeType = attrType;
  this->attrByteOffset = attrByteOffset;
  this->leafOccupancy = badgerdb::INTARRAYLEAFSIZE;
  this->nodeOccupancy = badgerdb::INTARRAYNONLEAFSIZE;

  // Scanning related memebers
  scanExecuting = false;
  this->nextEntry = INT_MAX;

  this->currentPageNum = Page::INVALID_NUMBER;
  this->currentPageData = nullptr;
  this->lowValInt = 0;
  this->lowValDouble = 0.0;
  this->lowValString = "";
  this->highValInt = 0;
  this->highValDouble = 0.0;
  this->highValString = "";
  this->lowOp = badgerdb::Operator::LTE;
  this->highOp = badgerdb::Operator::GTE;
  std::cout << "start constructor" << std::endl;

  try {
    File *file = new BlobFile(outIndexName, false);

    this->file = file;
    this->headerPageNum = file->getFirstPageNo();

    // Read the first page which contains the meta info
    badgerdb::Page *metaPage;  // headerpage
    this->bufMgr->readPage(file, this->headerPageNum, metaPage);
    badgerdb::IndexMetaInfo *meta = reinterpret_cast<IndexMetaInfo *>(metaPage);

    // Read root page number from the head (second page)
    this->rootPageNum = meta->rootPageNo;
    // Unpin the page after reading
    this->bufMgr->unPinPage(file, this->headerPageNum, false);
    std::cout << "start constructor 2" << std::endl;
  } catch (const badgerdb::FileNotFoundException &e) {
    // build the index
    File *file = new BlobFile(outIndexName, true);
    this->file = file;

    PageId headPageNum;
    PageId rootPageNum;
    Page *headPage;
    Page *rootPage;

    this->bufMgr->allocPage(file, headPageNum, headPage);
    this->bufMgr->allocPage(file, rootPageNum, rootPage);

    this->headerPageNum = headPageNum;

    badgerdb::IndexMetaInfo *metaInfo =
        reinterpret_cast<IndexMetaInfo *>(&headPage);

    strcpy(metaInfo->relationName, relationName.c_str());
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;

    this->bufMgr->unPinPage(this->file, headPageNum, true);

    // Insert and start to build the index
    // Scan the relation
    FileScan *scanner = new FileScan(relationName, this->bufMgr);
    std::cout << "start constructor3" << std::endl;
    try {
      // Read a record from the relation
      RecordId rid;
      scanner->scanNext(rid);

      std::string recordStr = scanner->getRecord();
      const char *record = recordStr.c_str();
      int key = *((int *)(record + attrByteOffset));

      // Root node starts as a leaf node
      badgerdb::LeafNodeInt *rootNode =
          reinterpret_cast<LeafNodeInt *>(&rootPage);
      this->ifRootIsLeaf = true;
      // initialzie the key and rid array
      size_t len = sizeof(rootNode->keyArray) / sizeof(rootNode->keyArray[0]);
      for (size_t i = 0; i < len; i++) {
        rootNode->keyArray[i] = INT_MAX;
      }

      // Assign key and rid for root
      rootNode->keyArray[0] = key;
      rootNode->ridArray[0] = rid;
      rootNode->rightSibPageNo = Page::INVALID_NUMBER;

      this->bufMgr->unPinPage(this->file, rootPageNum, true);
      this->bufMgr->flushFile(this->file);

      // THIS IS DANGEROUS, We Can do it only becuase scanner will
      // throw EndOfFileException when we reached the end of
      // the relation file
      std::cout << "before insert" << std::endl;
      while (1) {
        scanner->scanNext(rid);
        recordStr = scanner->getRecord();
        const char *recordChar = recordStr.c_str();
        int key = *((int *)(recordChar + attrByteOffset));
        this->insertEntry(&key, rid);
      }
      std::cout << "after insert" << std::endl;
    } catch (const EndOfFileException &e) {
      // Finish inserting all the records
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
  if (scanExecuting) {
    endScan();
  }
  // !!! Need all other pages closed
  this->bufMgr->flushFile(this->file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  const int *constIntKey = static_cast<const int *>(key);
  int intKey = *const_cast<int *>(constIntKey);

  RIDKeyPair<int> entry;
  entry.set(rid, intKey);
  PageKeyPair<int> *childEntry;

  // Read the root page
  Page *rootNode;
  this->bufMgr->readPage(this->file, this->rootPageNum, rootNode);

  if (this->ifRootIsLeaf) {
    insertHelper(rootNode, entry, childEntry, 0, true);
  } else {
    insertHelper(rootNode, entry, childEntry, 1, true);
  }
  this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  this->bufMgr->flushFile(this->file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertHelper()
// -----------------------------------------------------------------------------

void BTreeIndex::insertHelper(Page *pagePointer, RIDKeyPair<int> entry,
                              PageKeyPair<int> *childEntry, int pageLevel,
                              bool isRoot) {
  if (pageLevel > 0) {  // non-leaf node
    NonLeafNodeInt *node = (NonLeafNodeInt *)pagePointer;
    int index = 0;
    for (int i = 0; i < this->nodeOccupancy - 1; i++) {
      if (entry.key < node->keyArray[i]) {
        index = 0;
      } else if (entry.key >= node->keyArray[i] &&
                 entry.key < node->keyArray[i + 1]) {
        index = i;
      }
    }
    if (index == 0 && entry.key >= node->keyArray[this->nodeOccupancy - 1]) {
      index = this->nodeOccupancy;
    }
    Page *child;
    this->bufMgr->readPage(file, node->keyArray[index], child);
    insertHelper(child, entry, childEntry, node->level - 1, false);
    this->bufMgr->unPinPage(file, node->keyArray[index], true);

    if (childEntry == nullptr) {
      return;
    } else {
      if (node->keyArray[this->nodeOccupancy - 1] == INT_MAX) {  // space left
        // simply insert
        int insertIndex = this->nodeOccupancy - 1;
        while (insertIndex > 0 &&
               node->keyArray[insertIndex - 1] > childEntry->key) {
          node->keyArray[insertIndex] = node->keyArray[insertIndex - 1];
          node->pageNoArray[insertIndex + 1] = node->pageNoArray[insertIndex];
          insertIndex--;
        }
        node->keyArray[insertIndex] = childEntry->key;
        node->pageNoArray[insertIndex + 1] = node->pageNoArray[insertIndex];
        node->pageNoArray[insertIndex] = childEntry->pageNo;
        childEntry = nullptr;
      } else {  // no space left, need to split
        int leftSize = (this->nodeOccupancy + 1) / 2;
        int rightSize = this->nodeOccupancy + 1 - leftSize;

        PageId newPID;
        Page *newPage;
        this->bufMgr->allocPage(this->file, newPID, newPage);
        badgerdb::NonLeafNodeInt *newNode =
            reinterpret_cast<NonLeafNodeInt *>(newPage);

        // NonLeafNodeInt *newNode =
        // (NonLeafNodeInt *)malloc(sizeof(NonLeafNodeInt));
        if (node->keyArray[leftSize - 1] >
            childEntry->key) {  // child key belongs to left
          for (int i = 0; i < rightSize; i++) {
            newNode->keyArray[i] = node->keyArray[leftSize - 1 + i];
            newNode->pageNoArray[i] = node->pageNoArray[leftSize - 1 + i];
          }
          newNode->pageNoArray[rightSize] =
              node->pageNoArray[this->nodeOccupancy];
          for (int i = this->nodeOccupancy - 1; i >= leftSize - 1; i--) {
            node->keyArray[i] = INT_MAX;
            node->pageNoArray[i] = Page::INVALID_NUMBER;
          }
          node->pageNoArray[this->nodeOccupancy] = Page::INVALID_NUMBER;
          int insertIndex = leftSize - 1;
          while (insertIndex > 0 &&
                 node->keyArray[insertIndex - 1] > childEntry->key) {
            node->keyArray[insertIndex] = node->keyArray[insertIndex - 1];
            node->pageNoArray[insertIndex] = node->pageNoArray[insertIndex - 1];
            insertIndex--;
          }
          node->keyArray[insertIndex] = childEntry->key;
          node->pageNoArray[insertIndex] = childEntry->pageNo;
        } else {  // child key belongs to right;

          for (int i = 0; i < rightSize - 1; i++) {  // left one space for child
            newNode->keyArray[i] = node->keyArray[leftSize + i];
            newNode->pageNoArray[i] = node->pageNoArray[leftSize + i];
          }
          newNode->pageNoArray[rightSize - 1] =
              node->pageNoArray[this->nodeOccupancy];
          for (int i = this->nodeOccupancy - 1; i > leftSize - 1; i--) {
            node->keyArray[i] = INT_MAX;
            node->pageNoArray[i] = Page::INVALID_NUMBER;
          }
          node->pageNoArray[this->nodeOccupancy] = Page::INVALID_NUMBER;
          int insertIndex = rightSize - 1;
          while (insertIndex > 0 &&
                 node->keyArray[insertIndex - 1] > childEntry->key) {
            newNode->keyArray[insertIndex] = newNode->keyArray[insertIndex - 1];
            newNode->pageNoArray[insertIndex] =
                newNode->pageNoArray[insertIndex - 1];
            insertIndex--;
          }
          newNode->keyArray[insertIndex] = childEntry->key;
          newNode->pageNoArray[insertIndex] =
              newNode->pageNoArray[insertIndex - 1];
          newNode->pageNoArray[insertIndex] = childEntry->pageNo;
        }
        childEntry->set(newNode->pageNoArray[0], newNode->keyArray[0]);
        newNode->level = node->level;
        // set to default
        for (int i = rightSize; i < this->nodeOccupancy; i++) {
          newNode->keyArray[i] = INT_MAX;
          newNode->pageNoArray[i + 1] = Page::INVALID_NUMBER;
        }

        if (isRoot) {
          PageId rootPID;
          Page *newRootPage;
          this->bufMgr->allocPage(this->file, rootPID, newRootPage);
          badgerdb::NonLeafNodeInt *newRootNode =
              reinterpret_cast<NonLeafNodeInt *>(newRootPage);

          newRootNode->pageNoArray[0] = node->pageNoArray[0];
          newRootNode->pageNoArray[1] = childEntry->pageNo;
          newRootNode->keyArray[0] = childEntry->key;
          newRootNode->level = node->level + 1;
          this->rootPageNum = rootPID;
          this->ifRootIsLeaf = false;
          badgerdb::Page *metaPage;  // headerpage
          this->bufMgr->readPage(file, this->headerPageNum, metaPage);
          badgerdb::IndexMetaInfo *meta =
              reinterpret_cast<IndexMetaInfo *>(metaPage);

          meta->ifRootIsLeaf = false;
          // Unpin the page after reading
          this->bufMgr->unPinPage(file, this->headerPageNum, true);
          return;
        }

        this->bufMgr->unPinPage(this->file, newPID, newPage);
      }
    }

  } else {  // leaf node
    badgerdb::LeafNodeInt *node = reinterpret_cast<LeafNodeInt *>(pagePointer);

    if (node->keyArray[this->leafOccupancy - 1] == INT_MAX) {  // space left
      int insertIndex = this->leafOccupancy - 1;
      while (insertIndex > 0 &&
             node->keyArray[insertIndex - 1] > childEntry->key) {
        node->keyArray[insertIndex] = node->keyArray[insertIndex - 1];
        node->ridArray[insertIndex] = node->ridArray[insertIndex - 1];
        insertIndex--;
      }
      node->keyArray[insertIndex] = entry.key;
      node->ridArray[insertIndex] = entry.rid;
      childEntry = nullptr;
      return;
    } else {  // need to split
      int leftSize = (this->leafOccupancy + 1) / 2;
      int rightSize = this->leafOccupancy + 1 - leftSize;

      PageId newPID;
      Page *newPage;
      this->bufMgr->allocPage(this->file, newPID, newPage);
      badgerdb::LeafNodeInt *newNode = reinterpret_cast<LeafNodeInt *>(newPage);

      if (node->keyArray[leftSize - 1] > entry.key) {  // key belongs to left
        for (int i = 0; i < rightSize; i++) {
          newNode->keyArray[i] = node->keyArray[leftSize - 1 + i];
          newNode->ridArray[i] = node->ridArray[leftSize - 1 + i];
        }
        for (int i = this->leafOccupancy - 1; i >= leftSize - 1; i--) {
          node->keyArray[i] = INT_MAX;
          // We can leave the RID array uninitialized, just use key array to
          // check if that slot is valid node->ridArray[i] = nullptr;
        }
        int insertIndex = leftSize - 1;
        while (insertIndex > 0 && node->keyArray[insertIndex - 1] > entry.key) {
          node->keyArray[insertIndex] = node->keyArray[insertIndex - 1];
          node->ridArray[insertIndex] = node->ridArray[insertIndex - 1];
          insertIndex--;
        }
        node->keyArray[insertIndex] = entry.key;
        node->ridArray[insertIndex] = entry.rid;
      } else {  // child key belongs to right;

        for (int i = 0; i < rightSize - 1; i++) {  // left one space for child
          newNode->keyArray[i] = node->keyArray[leftSize + i];
          newNode->ridArray[i] = node->ridArray[leftSize + i];
        }

        for (int i = this->leafOccupancy - 1; i > leftSize - 1; i--) {
          node->keyArray[i] = INT_MAX;
          // node->ridArray[i] = nullptr;
        }
        int insertIndex = rightSize - 1;
        while (insertIndex > 0 && node->keyArray[insertIndex - 1] > entry.key) {
          newNode->keyArray[insertIndex] = newNode->keyArray[insertIndex - 1];
          newNode->ridArray[insertIndex] = newNode->ridArray[insertIndex - 1];
          insertIndex--;
        }
        node->keyArray[insertIndex] = entry.key;
        node->ridArray[insertIndex] = entry.rid;
      }
      /// TODO: Leaf node has no pageNoArray
      childEntry->set(newNode->keyArray[0], newPID);
      // set to default
      for (int i = rightSize; i < this->leafOccupancy; i++) {
        newNode->keyArray[i] = INT_MAX;
        // newNode->ridArray[i + 1] = nullptr;
      }
      newNode->rightSibPageNo = node->rightSibPageNo;
      node->rightSibPageNo = newPID;
      if (isRoot) {
        PageId rootPID;
        Page *newRootPage;
        this->bufMgr->allocPage(this->file, rootPID, newRootPage);
        badgerdb::NonLeafNodeInt *newRootNode =
            reinterpret_cast<NonLeafNodeInt *>(newRootPage);

        newRootNode->pageNoArray[0] = this->rootPageNum;
        newRootNode->pageNoArray[1] = childEntry->pageNo;
        newRootNode->keyArray[0] = childEntry->key;
        newRootNode->level = 1;
        this->rootPageNum = rootPID;
        this->ifRootIsLeaf = false;
        badgerdb::Page *metaPage;  // headerpage
        this->bufMgr->readPage(file, this->headerPageNum, metaPage);
        badgerdb::IndexMetaInfo *meta =
            reinterpret_cast<IndexMetaInfo *>(metaPage);

        meta->ifRootIsLeaf = false;
        // Unpin the page after reading
        this->bufMgr->unPinPage(file, this->headerPageNum, true);
        }
      this->bufMgr->unPinPage(this->file, newPID, true);
      return;
    }
  }
}

void BTreeIndex::search(PageId &foundPageID, PageId currPageId, const void *key,
                        std::vector<PageId> &path) {
  Page *currPage;
  this->bufMgr->readPage(file, currPageId, currPage);
  NonLeafNodeInt *currNode = (NonLeafNodeInt *)currPage;

  int idx = 0;
  int keyC = *(int *)key;

  // while (idx < currNode->numOfKey && currNode->keyArray[idx] <= keyC) {
  //   idx++;
  // }

  for (int i = 0; i < this->nodeOccupancy; i++) {
    if (currNode->keyArray[i] > keyC) {
      idx = i;
      break;
    }
  }

  if (currNode->level == 1) {
    foundPageID = currNode->pageNoArray[idx];
    path.push_back(currPageId);
  } else {
    path.push_back(currPageId);
    search(foundPageID, currNode->pageNoArray[idx], key, path);
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm,
                           const void *highValParm, const Operator highOpParm) {
  
  if (highOpParm != LT && highOpParm != LTE) {
    throw BadOpcodesException();
  }
  
  if (lowOpParm != GT && lowOpParm != GTE) {
    throw BadOpcodesException();
  }

  if (!scanExecuting) {
    endScan();
  }

  // initilizing fields
  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);
  lowOp = lowOpParm;
  highOp = highOpParm;

  if (highValInt < lowValInt) {
    throw BadScanrangeException();
  }

  PageId fid;
  std::vector<PageId> path;

  if (this->ifRootIsLeaf) {
    fid = rootPageNum;
  } else {
    search(fid, rootPageNum, lowValParm, path);
  }

  Page *fpage;
  bufMgr->readPage(file, fid, fpage);
  LeafNodeInt *fnode = (LeafNodeInt *)fpage;

  int idx = -1;
  for (int i = 0; i < this->leafOccupancy; i++) {
    if (lowValInt <= foundNode->keyArray[i]) {
      idx = i;
      break;
    }
  }

  while (idx == -1 && fnode->rightSibPageNo != Page::INVALID_NUMBER) {
    bufMgr->readPage(file, fnode->rightSibPageNo, fpage);
    bufMgr->unPinPage(file, fid, false);
    fid = fnode->rightSibPageNo;
    fnode = (LeafNodeInt *)foundPage;
    // bufMgr->unPinPage(file, fnode->rightSibPageNo, false);

    for (int i = 0; i < this->leafOccupancy; i++) {
      if (lowValInt <= fnode->keyArray[i]) {
        idx = i;
        break;
      }
    }
  }

  if (idx == -1) {
    bufMgr->unPinPage(file, fnode->rightSibPageNo, false);
    throw NoSuchKeyFoundException();
  }

  if (lowOpParm == GT) {
    if (fnode->keyArray[idx] > lowValInt) {
      currentPageData = fpage;
      currentPageNum = fid;
      nextEntry = idx;
    } else {
      if (idx <= this->leafOccupancy - 2) {
        currentPageData = foundPage;
        currentPageNum = fid;
        nextEntry = idx + 1;
      } else {
        if (foundNode->rightSibPageNo != Page::INVALID_NUMBER) {
          currentPageData = foundPage;
          currentPageNum = foundNode->rightSibPageNo;
          nextEntry = 0;
        } else {
          // bufMgr->unPinPage(file, foundNode->rightSibPageNo, false);
          throw NoSuchKeyFoundException();
        }
      }
    }
  } else {
    currentPageData = foundPage;
    currentPageNum = fid;
    nextEntry = idx;
  }
  scanExecuting = true;
  bufMgr->unPinPage(file, foundNode->rightSibPageNo, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId &outRid) {
  if (scanExecuting == false) {
    throw ScanNotInitializedException();
  }

  bufMgr->readPage(file, currentPageNum, currentPageData);
  LeafNodeInt *currPage = NULL;
  if (currentPageData != NULL) {
    currPage = (LeafNodeInt *)currentPageData;
    int currKey = currPage->keyArray[nextEntry];
    outRid = currPage->ridArray[nextEntry];

    if ((currKey > highValInt && (highOp == LTE || highOp == LT)) ||
        (currKey == highValInt && highOp == LT)) {
      bufMgr->unPinPage(file, currentPageNum, false);
      throw IndexScanCompletedException();
    }

    int end = this->leafOccupancy - 1;
    if (nextEntry < end) {
      nextEntry = nextEntry + 1;
    } else {
      if (currPage->rightSibPageNo == Page::INVALID_NUMBER) {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw IndexScanCompletedException();
      }
      currentPageNum = currPage->rightSibPageNo;
      nextEntry = 0;
    }

  } else {
    endScan();
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
  if (scanExecuting == false) {
    throw ScanNotInitializedException();
  }

  scanExecuting = false;

  if (currentPageNum != Page::INVALID_NUMBER) {
    bufMgr->unPinPage(file, currentPageNum, false);
  }

  lowOp = LT;
  highOp = GT;
  lowValInt = -1;
  highValInt = -1;
  nextEntry = -1;
  currentPageData = NULL;
  currentPageNum = Page::INVALID_NUMBER;
}

}  // namespace badgerdb
