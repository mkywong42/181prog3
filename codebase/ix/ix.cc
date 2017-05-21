
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return IX_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return IX_MALLOC_FAILED;
    newIndexPage(firstPageData);
    NodeHeader nodeHeader = getNodePageHeader(firstPageData);
    nodeHeader.isRoot = true;
    nodeHeader.isLeaf = true;
    setNodePageHeader(firstPageData, nodeHeader);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return IX_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return IX_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //find correct leaf page
    unsigned pageNum = traverse(ixfileHandle, attribute, key);
    void* pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.fileHandle.readPage(pageNum, pageData))
        return IX_READ_FAILED;
    NodeHeader nodeHeader = getNodePageHeader(pageData);
    //if not enough size
    if(nodeHeader.endOfEntries+sizeof(NodeHeader)>PAGE_SIZE){
        //split
    }else{
        //insert in sorted order
        //find correct place
        unsigned entryNum = findPointerEntry(pageData, attribute, key);
        unsigned offset = sizeof(NodeHeader) + entryNum * sizeof(NodeEntry);
        //insert entry
        NodeEntry entry;
        entry.rid = rid;
        entry.key = *((int*)key);               ////////////wrong
        entry.leftChildPageNum = 0;
        entry.rightChildPageNum = 0;
        setEntryAtOffset(pageData, offset, entry);
        //update node page header
        nodeHeader.indexEntryNumber++;
        nodeHeader.endOfEntries+=sizeof(NodeEntry);
        setNodePageHeader(pageData, nodeHeader);
    }
    ixfileHandle.fileHandle.writePage(pageNum,pageData);
    free(pageData);
    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0; 
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    if(fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter)){
        return IXF_COLLECT_VALUES_FAIL;
    }

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

void IndexManager::newIndexPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    NodeHeader nodeHeader;
    nodeHeader.endOfEntries = sizeof(NodeHeader);
    nodeHeader.indexEntryNumber = 0;
    nodeHeader.isLeaf = false;
    nodeHeader.isRoot = false;
    nodeHeader.leftPageNum = 0;
    nodeHeader.rightPageNum = 0;
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

NodeHeader IndexManager::getNodePageHeader(void * page){
    NodeHeader nodeHeader;
    memcpy (&nodeHeader, page, sizeof(NodeHeader));
    return nodeHeader;
}

void IndexManager::setNodePageHeader(void * page, NodeHeader nodeHeader){
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

NodeEntry IndexManager::getNodeEntry(void* page, unsigned entryNum){
    NodeEntry entry;
    unsigned offset = sizeof(NodeHeader) + entryNum * sizeof(NodeEntry);
    memcpy(&entry, (char*)page + offset, sizeof(NodeEntry));
    return entry;
}

unsigned IndexManager::getRootPageNum(IXFileHandle ixFileHandle){
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return IX_MALLOC_FAILED;
    unsigned i;
    unsigned numPages = ixFileHandle.fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (ixFileHandle.fileHandle.readPage(i, pageData))
            return IX_READ_FAILED;  
        
        NodeHeader nodeHeader = getNodePageHeader(pageData);
        if(nodeHeader.isRoot == true){
            break;
        }
    }
    free(pageData);
    return i;
}

unsigned IndexManager::traverse(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key){
    bool atLeaf = false;
    unsigned currentPage = getRootPageNum(ixfileHandle);
    while(atLeaf == false){
        void* pageData = malloc(PAGE_SIZE);
        if (ixfileHandle.fileHandle.readPage(currentPage, pageData) != SUCCESS)
        {
            free(pageData);
            return IX_READ_FAILED;
        }
        NodeHeader nodeHeader = getNodePageHeader(pageData);
        if(nodeHeader.isLeaf == true){
            atLeaf = true;
            free(pageData);
            break;
        }
        //find correct entry
        unsigned entryNumber = findPointerEntry(pageData, attribute, key);
        //decide where to go
        NodeEntry entry = getNodeEntry(pageData, entryNumber);
        int result = compare(attribute,entry, key);
        //set current page
        if(result <1){
            currentPage = entry.leftChildPageNum;
        }else{
            currentPage = entry.rightChildPageNum;
        }
        free(pageData);
        //need more??-------------------------------------------
    }
    return currentPage;
}

unsigned IndexManager::findPointerEntry(void* page, const Attribute &attribute, const void *key){
    NodeHeader nodeHeader = getNodePageHeader(page);
    unsigned i;
    for(i=0; i<nodeHeader.indexEntryNumber; i++){
        NodeEntry entry = getNodeEntry(page, i);
        unsigned result = compare(attribute, entry, key);
        if(result > 0 || i==nodeHeader.indexEntryNumber-1){
            break;
        }
    }
    return i;
}

//returns -1 if entry.key is less than key, returns 1 otherwise
int IndexManager::compare(const Attribute &attribute, NodeEntry entry, const void *key){
    if(attribute.type == TypeVarChar){
        //TODO
    }else if(attribute.type == TypeReal){
        if(entry.key<*((float*)key)){return -1;}
        else {return 1;}                            //need to define key
    }else{
        if(entry.key<*((int*)key)){ return -1;}     //need to define key
        else {return 1;}
    }
    return 0;

}

void IndexManager::setEntryAtOffset(void* page, unsigned offset, NodeEntry entry){
    NodeHeader nodeHeader = getNodePageHeader(page);
    unsigned movingBlockSize = nodeHeader.endOfEntries - offset;
    memmove((char*)page+offset+sizeof(NodeEntry), (char*)page+offset, movingBlockSize);
    memcpy((char*)page + offset, &entry, sizeof(NodeEntry));
}
