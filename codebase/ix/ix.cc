
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

    // void * headerBuffer = malloc(sizeof(BTreeHeader));
    // BTreeHeader bHeader;
    // bHeader.rootPageNum = 0;
    // memcpy(headerBuffer, &bHeader, sizeof(BTreeHeader));
    // fwrite(headerBuffer, sizeof(BTreeHeader), 1, fileName);     //check------------------

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return IX_MALLOC_FAILED;
    newIndexPage(firstPageData);
    NodeHeader nodeHeader = getNodePageHeader(firstPageData);
    nodeHeader.isRoot = true;
    nodeheader.isLeaf = true;
    setNodePageHeader(firstPageData, nodeHeader);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return IX_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return IX_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(headerBuffer);
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
    return -1;
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
    fileHandle = NULL;
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
    nodeHeader.freeSpaceOffset = sizeof(NodeHeader);
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
    return NodeHeader;
}

void IndexManager::setNodePageHeader(void * page, NodeHeader nodeHeader){
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

NodeEntry IndexManager::getNodeEntry(void* page, unsigned pageNum){
    NodeEntry entry;
    unsigned offset = sizeof(NodeHeader) + pageNum * sizeof(NodeEntry);
    memcpy(&entry, (char*)page + offset, sizeof(NodeEntry));
    return SUCCESS;
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