/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
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
	this->nextEntry = 0; // Need to confirm
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

		this->currentPageNum = this->rootPageNum;
		this->bufMgr->readPage(file, this->rootPageNum, this->currentPageData);
	}
	catch(const badgerdb::FileNotFoundException & e)
	{
		// build the index
		BlobFile newIndexFile = BlobFile::create(outIndexName);
		File *file = &bfile;
		this->file = file;

		PageId headPageNum;
		PageId rootPageNum;
		Page *headPage;
		Page *rootPage;

		this->bufMgr->allocPage(file, headPageNum, headPage);
		this->bufMgr->allocPage(file, rootPageNum, rootPage);

		this->headerPageNum = headPageNum;
		badgerdb::IndexMetaInfo *metaInfo;
		metaInfo->relationName = relationName;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;

		// write meta data to head page, unpinned
		// We won't use it latter
		headPage = reinterpret_cast<Page *>(metaInfo);
		this->bufMgr->unPinPage(file, headPageNum, true);
		
		// Insert and start to build the index
		FileScan scanner = new FileScan(relationName, this->bufMgr);
		try
		{
			RecordId *rid;
			scanner.scanNext(rid);

			std::string record = scanner.getRecord();
			char *tempRecord = record;
			int key = &tempRecord[0] + this->attrByteOffset;
			// todo: figure out if root is leaf node or non-leaf node
			// todo: insert records
		}
		catch(const babadgerdb::EndOfFileException n& e)
		{
			
		}
		
		
		// unpin all the pages in the buffer pool before do this
		this->bufMgr->flushFile(file);
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	this->bufMgr->unPinPage(this->file, this->currentPageData, true);
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
