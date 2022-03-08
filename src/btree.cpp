/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include <vector>
#include <climits>
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();
	outIndexName = indexName;

	this->bufMgr = bufMgr;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->leafOccupancy = badgerdb::INTARRAYLEAFSIZE;
	this->nodeOccupancy = badgerdb::INTARRAYNONLEAFSIZE;
	
	// Scanning related memebers
	this->scanExecuting = false;
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
	
	
	try
	{
		BlobFile bfile = BlobFile::open(outIndexName);
		File *file = &bfile;

		this->file = file;
		this->headerPageNum = bfile.getFirstPageNo();

		// Read the first page which contains the meta info
		badgerdb::Page *metaPage; //headerpage
		this->bufMgr->readPage(file, this->headerPageNum, metaPage); 
		badgerdb::IndexMetaInfo *meta = reinterpret_cast<IndexMetaInfo *>(metaPage);

		// Read root page number from the head (second page)
		this->rootPageNum = meta->rootPageNo;
		// Unpin the page after reading
		this->bufMgr->unPinPage(file, this->headerPageNum, false);
	}
	catch(const badgerdb::FileNotFoundException & e)
	{
		// build the index
		BlobFile newIndexFile = BlobFile::create(outIndexName);
		File *file = &newIndexFile;
		this->file = file;

		PageId headPageNum;
		PageId rootPageNum;
		Page *headPage;
		Page *rootPage;

		this->bufMgr->allocPage(file, headPageNum, headPage);
		this->bufMgr->allocPage(file, rootPageNum, rootPage);

		this->headerPageNum = headPageNum;

		badgerdb::IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(headPage);

		strcpy(metaInfo->relationName, relationName.c_str());
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;

		// write meta data to head page, unpinned
		// We won't use it latter
		this->bufMgr->unPinPage(file, headPageNum, true);
		
		// Insert and start to build the index
		// Scan the relation
		FileScan *scanner = new FileScan(relationName, this->bufMgr);

		try
		{
			// Read a record from the relation
			RecordId rid;
			scanner->scanNext(rid);
			std::string record = scanner->getRecord();

			std::vector<char> writable(record.begin(), record.end());
			writable.push_back('\0');
			char *tempRecord = &writable[0];

			void *key = reinterpret_cast<int *>(&tempRecord[0] + this->attrByteOffset);
			int intKey = *reinterpret_cast<int*>(key);
			
			// Root node starts as a leaf node
			badgerdb::LeafNodeInt *rootNode = reinterpret_cast<LeafNodeInt *>(rootPage);
			// initialzie the key and rid array
			size_t len = sizeof(rootNode->keyArray)/sizeof(rootNode->keyArray[0]);
			for (size_t i = 0; i < len; i++)
			{
				rootNode->keyArray[i] = INT_MAX;
			}
			
			// Assign key and rid for root
			rootNode->keyArray[0] = intKey;
			rootNode->ridArray[0] = rid;
			rootNode->rightSibPageNo = Page::INVALID_NUMBER;

			this->bufMgr->unPinPage(file, rootPageNum, true);
			this->bufMgr->flushFile(file);

			// THIS IS DANGEROUS, We Can do it only becuase scanner will 
			// throw EndOfFileException when we reached the end of 
			// the relation file
			while(1) {
				scanner->scanNext(rid);
				record = scanner->getRecord();
				std::vector<char> writable(record.begin(), record.end());
				writable.push_back('\0');
				tempRecord = &writable[0];
				
				key = reinterpret_cast<int *>(&tempRecord[0] + this->attrByteOffset);

				this->insertEntry(key, rid);
			}
		}
		catch(const EndOfFileException &e)
		{
			// Finish inserting all the records
		}
		
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	if(scanExecuting) {
		endScan();
	}
	// !!! Need all other pages closed
	this->bufMgr->flushFile(this->file);

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	if(lowOpParm != GT && lowOpParm != GTE){
        throw BadOpcodesException();
    }

    if(lowOpParm != LT && lowOpParm != LTE){
        throw BadOpcodesException();
    }

    if(scanExecuting){
		endScan();
	}

	// initilizing fields
	lowValInt = *((int *)lowValParm);
	highValInt = *((int *)highValParm);
	lowOp = lowOpParm;
	highOp = highOpParm;

	if(highValInt < lowValInt){
		throw BadScanrangeException();
	}

	PageId foundId;
	std::vector<PageId> path;

	if(firstRoot){
		foundId = rootPageNum;
	}else{
		search(foundId, rootPageNum, lowValParm, path);
	}

	Page *foundPage;
	bufMgr->readPage(file, foundId, foundPage);
	LeafNodeInt *foundNode = (LeafNodeInt *)foundPage;

	bufMgr->unPinPage(file, foundId, false);

	int idx = -1;
	for(int i = 0; i < foundNode->numOfKey; i++){
		if(lowValInt <= foundNode->keyArray[i]){
			idx = i;
			break;
		}
	}

	while(idx == -1 && foundNode->rightSibPageNo != Page::INVALID_NUMBER){
		bufMgr->readPage(file, foundNode->rightSibPageNo, foundPage);
		foundId = foundNode->rightSibPageNo;
		foundNode = (LeafNodeInt *)foundPage;
		bufMgr->unPinPage(file, foundNode->rightSibPageNo, false);

		for(int i = 0; i < foundNode->numOfKey; i++){
			if(lowValInt <= foundNode->keyArray[i]){
				idx = i;
				break;
			}
		}
	}

	if(idx = -1){
		throw NoSuchKeyFoundException();
	}

	if(lowOpParm == GT){
		if(foundNode->keyArray[idx] > lowValInt){
			currentPageData = foundPage;
			currentPageNum = foundId;
			nextEntry = idx;
		}else{
			if(idx <= foundNode->numOfKey - 2){
				currentPageData = foundPage;
				currentPageNum = foundId;
				nextEntry = idx + 1;
			}else{
				if(foundNode->rightSibPageNo != Page::INVALID_NUMBER){
					currentPageData = foundPage;
					currentPageNum = foundNode->rightSibPageNo;
					nextEntry = 0;
				}else{
					throw NoSuchKeyFoundException();
				}
			}
		}
	}else{
		currentPageData = foundPage;
		currentPageNum = foundId;
		nextEntry = idx;
	}
	scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}

	bufMgr-> readPage(file,currentPageNum, currentPageData);
	LeafNodeInt *currPage = NULL;
	if(currentPageData != NULL){

		currPage = (LeafNodeInt *)currentPageData;
		int currKey = currPage->keyArray[nextEntry];
		outRid = currPage->ridArray[nextEntry];

		if((currKey > highValInt && (highOp == LTE || highOp == LT)) || (currKey == highValInt && highOp == LT)){
			throw IndexScanCompletedException();
		}

		int end;
		end = currPage->numOfKey - 1;
		if(nextEntry < end){
			nextEntry = nextEntry + 1;
			bufMgr->unPinPage(file, currentPageNum, false);
		}else{
			if(currPage->rightSibPageNo == Page::INVALID_NUMBER){
				throw IndexScanCompletedException();
			}
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = currPage->rightSibPageNo;
			nextEntry = 0;
		}

	}else{
		endScan();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}

	scanExecuting = false;
	
	if(currentPageNum != Page::INVALID_NUMBER){
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

void BTreeIndex::search(PageId &foundPageID, PageId currPageId, const void *key, std::vector<PageId> &path){
	Page *currPage;
	BufMgr->readPage(file, currPageId, currPage);
	NonLeafNodeInt *currNode = (NonLeafNodeInt *)currPage;

	int idx = 0;
	int keyC = *(int *)key;

	while (idx < currNode->numOfKey && currNode->keyArray[idx] <= keyC){
		idx++
	}
	
	if(currNode->level == 1){
		foundPageID = currNode->pageNoArray[idx];
		path.push_back(currPageId);
	}else{
		path.push_back(currPageId);
		search(foundPageID, currNode->pageNoArray[idx], key, path);
	}
}


}
