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
		badgerdb::Page *metaPage;
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
	if(this->currentPageData != nullptr && this->currentPageNum != Page::INVALID_NUMBER) {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	}
	// !!! Need all other pages closed
	this->bufMgr->flushFile(this->file);

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	RIDKeyPair<int> entry;
	entry.set(key, rid);
	PageKeyPair<int>* childEntry = (PageKeyPair<int>*)malloc(sizeof(PageKeyPair<int>));
	childEntry = nullptr;
	if(this->ifRootIsLeaf){
		insertHelper(rootNode, entry, childEntry, 0,true);
	}else{
		insertHelper(rootNode, entry, childEntry, 1,true);
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertHelper()
// -----------------------------------------------------------------------------

void BTreeIndex::insertHelper(Page* pagePointer, RIDKeyPair<int> entry, PageKeyPair<int>* childEntry, int pageLevel, bool isRoot) 
{
	if(pageLevel>0){ // non-leaf node
		NonLeafNodeInt* node = (NonLeafNodeInt*) pagePointer;
		int index = 0;
		for(int i = 0; i<this->nodeOccupancy-1; i++){
			if(entry.key<node->keyArray[i]){
				index = 0;
			}else if(entry.key>=node->keyArray[i]&&entry.key<node->keyArray[i+1]){
				index = i;
			}
		}
		if(index==0&&entry.key>=node->keyArray[this->nodeoccupancy-1]){
			index = this->nodeoccupancy;
		}
		Page* child;
		this->bufMgr->readPage(file, node->keyArray[index], child);
		insertHelper(child, entry, *childEntry, node->level-1, false);
		if(childEntry==nullptr){
			return;
		}else{
			if(node->keyArray[this->nodeOccupancy-1]==INT_MAX){ // space left
				// simply insert
				int insertIndex = this->nodeOccupancy-1;
				while(insertIndex>0&&node->keyArray[insertIndex-1]>childEntry.key){
					node->keyArray[insertIndex] = node->keyArray[insertIndex-1];
					node->pageNoArray[insertIndex+1] = node->pageNoArray[insertIndex];
					insertIndex--;
				}
				node->keyArray[insertIndex]= childEntry.key;
				node->pageNoArray[insertIndex+1] = node->pageNoArray[insertIndex];
				node->pageNoArray[insertIndex] = childEntry.pageNo;
				childEntry = nullptr;
			}else{ // no space left, need to split
				int leftSize = (this->nodeOccupancy+1)/2;
				int rightSize = this->nodeOccupancy+1 - leftSize;
				NonLeafNodeInt* newNode = (NonLeafNodeInt*)malloc(sizeof(NonLeafNodeInt));
				if(node->keyArray[leftSize-1]>childEntry.key){ // child key belongs to left
					for(int i = 0; i < rightSize; i++){
						newNode->keyArray[i] = node->keyArray[leftSize-1+i];
						newNode->pageNoArray[i] = node->pageNoArray[leftSize-1+i];
					}
					newNode->pageNoArray[rightSize] = node->pageNoArray[this->nodeOccupancy];
					for(int i = this->nodeOccupancy-1; i>=leftSize-1; i--){
						node->keyArray[i] = INT_MAX;
						node->pageNoArray[i] = nullptr;
					}
					node->pageNoArray[this->nodeOccupancy] = nullptr;
					int insertIndex = leftSize-1;
					while(insertIndex>0&&node->keyArray[insertIndex-1]>childEntry.key){
						node->keyArray[insertIndex] = node->keyArray[insertIndex-1];
						node->pageNoArray[insertIndex] = node->pageNoArray[insertIndex-1];
						insertIndex--;
					}
					node->keyArray[insertIndex]= childEntry.key;
					node->pageNoArray[insertIndex] = childEntry.pageNo;
				}else{ // child key belongs to right;

					for(int i = 0; i < rightSize-1; i++){ // left one space for child
						newNode->keyArray[i] = node->keyArray[leftSize+i];
						newNode->pageNoArray[i] = node->pageNoArray[leftSize+i]																										1+i];
					}
					newNode->pageNoArray[rightSize-1] = node->pageNoArray[this->nodeOccupancy];
					for(int i = this->nodeOccupancy-1; i>leftSize-1; i--){
						node->keyArray[i] = INT_MAX;
						node->pageNoArray[i] = nullptr;
					}
					node->pageNoArray[this->nodeOccupancy] = nullptr;
					int insertIndex = rightSize-1;
					while(insertIndex>0&&node->keyArray[insertIndex-1]>childEntry.key){
						newNode->keyArray[insertIndex] = newNode->keyArray[insertIndex-1];
						newNode->pageNoArray[insertIndex] = newNode->pageNoArray[insertIndex-1];
						insertIndex--;
					}
					newNode->keyArray[insertIndex]= childEntry.key;
					newNode->pageNoArray[insertIndex] = newNode->pageNoArray[insertIndex-1];
					newNode->pageNoArray[insertIndex] = childEntry.pageNo;
				}
				childEntry.Set(newNode->keyArray[0], newNode->pageNoArray[0]);
				newNode->level = node->level;
				// set to default
				for(int i = rightSize; i <this->nodeOccupancy; i++){
						newNode->keyArray[i] = INT_MAX;
						newNode->pageNoArray[i+1] = nullptr;
				}

				if(isRoot){
					NonLeafNodeInt* newRootNode = (NonLeafNodeInt*)malloc(sizeof(NonLeafNodeInt));
					newRootNode->pageNoArray[0] = node->pageNoArray[0];
					newRootNode->pageNoArray[1] = childEntry.pageNo;
					newRootNode->keyArray[0] = childEntry.key;
					newRootNode->level = node->level+1;
					this->rootNode = newRootNode;
					this->ifRootIsLeaf = false;
					return;
				}
			}
		}
		
		
	}else{ // leaf node
		LeafNodeInt* node = (NonLeafNodeInt*) pagePointer;
		if(node->keyArray[this->leafOccupancy-1]==INT_MAX){ // space left
			int insertIndex = this->leafOccupancy-1;
			while(insertIndex>0&&node->keyArray[insertIndex-1]>childEntry.key){
				node->keyArray[insertIndex] = node->keyArray[insertIndex-1];
				node->ridArray[insertIndex] = node->ridArray[insertIndex-1];
				insertIndex--;
			}
			node->keyArray[insertIndex]= entry.key;
			node->ridArray[insertIndex] = entry.rid;
			childEntry = nullptr;
			return;
		}else{ // need to split
			int leftSize = (this->leafOccupancy+1)/2;
			int rightSize = this->leafOccupancy+1 - leftSize;
			LeafNodeInt* newNode = (LeafNodeInt*)malloc(sizeof(LeafNodeInt));

			if(node->keyArray[leftSize-1]>entry.key){ // key belongs to left
				for(int i = 0; i < rightSize; i++){
					newNode->keyArray[i] = node->keyArray[leftSize-1+i];
					newNode->ridArray[i] = node->ridArray[leftSize-1+i];
				}
				for(int i = this->leafOccupancy-1; i>=leftSize-1; i--){
					node->keyArray[i] = INT_MAX;
					node->ridArray[i] = nullptr;
				}
				int insertIndex = leftSize-1;
				while(insertIndex>0&&node->keyArray[insertIndex-1]>entry.key){
					node->keyArray[insertIndex] = node->keyArray[insertIndex-1];
					node->ridArray[insertIndex] = node->ridArray[insertIndex-1];
					insertIndex--;
				}
				node->keyArray[insertIndex]= entry.key;
				node->ridArray[insertIndex] = entry.rid;
			}else{ // child key belongs to right;

				for(int i = 0; i < rightSize-1; i++){ // left one space for child
					newNode->keyArray[i] = node->keyArray[leftSize+i];
					newNode->ridArray[i] = node->ridArray[leftSize+i]																										1+i];
				}

				for(int i = this->leafOccupancy-1; i>leftSize-1; i--){
					node->keyArray[i] = INT_MAX;
					node->ridArray[i] = nullptr;
				}
				int insertIndex = rightSize-1;
				while(insertIndex>0&&node->keyArray[insertIndex-1]>entry.key){
					newNode->keyArray[insertIndex] = newNode->keyArray[insertIndex-1];
					newNode->ridArray[insertIndex] = newNode->ridArray[insertIndex-1];
					insertIndex--;
				}
				node->keyArray[insertIndex]= entry.key;
				node->ridArray[insertIndex] = entry.rid;
			}
			childEntry.Set(newNode->keyArray[0], newNode->pageNoArray[0]);
			// set to default
			for(int i = rightSize; i <this->leafOccupancy; i++){
				newNode->keyArray[i] = INT_MAX;
				newNode->ridArray[i+1] = nullptr;
			}
			newNode->rightSibPageNo = node->rightSibPageNo;
			node->rightSibPageNo = newNode;
			return;


		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
