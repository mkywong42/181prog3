
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
// cout<<*((int*)key)<<"pageNum: "<<pageNum<<endl;
    void* pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.fileHandle.readPage(pageNum, pageData))
        return IX_READ_FAILED;
    NodeHeader nodeHeader = getNodePageHeader(pageData);
    //if not enough size
    if(nodeHeader.endOfEntries+sizeof(NodeEntry)>PAGE_SIZE){
        splitPage(ixfileHandle, pageData, pageNum,nodeHeader.parent,attribute, key, rid);
        // insertEntry(ixfileHandle, attribute, key, rid);
    }else{
        insertInSortedOrder(pageData, attribute, key, rid, -1, -1);
        if(ixfileHandle.fileHandle.writePage(pageNum,pageData))
            return IX_WRITE_FAILED;
    }
// void* testPageData = malloc(PAGE_SIZE);
// ixfileHandle.fileHandle.readPage(0,testPageData);
// NodeHeader testHeader = getNodePageHeader(testPageData);
// cout<<testHeader.indexEntryNumber<<endl;
// cout<<testHeader.isLeaf<<endl;
// // cout<<testHeader.isRoot<<endl;
// NodeEntry tester = getNodeEntry(testPageData, 0);
// cout<<tester.key.intValue<<endl;
// cout<<tester.rid.pageNum<<endl;
// cout<<tester.rid.slotNum<<endl;
// printBtree(ixfileHandle, attribute);
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
    unsigned rootPage = getRootPageNum(ixfileHandle);
    printRecursively(ixfileHandle, attribute, rootPage); 
}

void IndexManager::printRecursively(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageNum)const{
    if(pageNum<0){
        return;          //check ----------------------------------
    }
    void* pageData = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageNum, pageData);
    NodeHeader nodeHeader = getNodePageHeader(pageData);
    if(nodeHeader.isLeaf==false){
        cout<<"{\"keys\": [";
        for(unsigned i = 0; i<nodeHeader.indexEntryNumber; i++){
            cout<<"\"";
            NodeEntry temp = getNodeEntry(pageData, i);
            if(attribute.type==TypeVarChar){
                cout<<temp.key.strValue;
            }else if(attribute.type == TypeReal){
                cout<<temp.key.floatValue;
            }else{
                cout<<temp.key.intValue;
            }
            cout<<"\"";
            if(i != nodeHeader.indexEntryNumber-1) cout<<",";
        }
        cout<<"],"<<endl;
        cout<<"\"children\":["<<endl;
        // printRecursively(ixfileHandle, attribute, nodeHeader.leftChildPageNum);
        // printRecursively(ixfileHandle, attribute, nodeHeader.rightChildPageNum);
        cout<<"]}"<<endl;
    }else{
        cout<<"{\"keys\": [";
        for(unsigned i = 0; i<nodeHeader.indexEntryNumber;i++){
            cout<<"\"";
            NodeEntry temp = getNodeEntry(pageData,i);
            if(attribute.type==TypeVarChar){
                cout<<temp.key.strValue;
            }else if(attribute.type == TypeReal){
                cout<<temp.key.floatValue;
            }else{
                cout<<temp.key.intValue;
            }
            cout<<":[("<<temp.rid.pageNum<<","<<temp.rid.slotNum<<")]\"";
            if(i!=nodeHeader.indexEntryNumber-1) cout<<",";
        }
        cout<<"}"<<endl;
    }
    free(pageData);
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
    nodeHeader.leftPageNum = -1;
    nodeHeader.rightPageNum = -1;
    nodeHeader.parent = -1;
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

NodeHeader IndexManager::getNodePageHeader(void * page)const{
    NodeHeader nodeHeader;
    memcpy (&nodeHeader, page, sizeof(NodeHeader));
    return nodeHeader;
}

void IndexManager::setNodePageHeader(void * page, NodeHeader nodeHeader){
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

NodeEntry IndexManager::getNodeEntry(void* page, unsigned entryNum)const{
    NodeEntry entry;
    unsigned offset = sizeof(NodeHeader) + entryNum * sizeof(NodeEntry);
    memcpy(&entry, (char*)page + offset, sizeof(NodeEntry));
    return entry;
}

unsigned IndexManager::getRootPageNum(IXFileHandle ixfileHandle)const{
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return IX_MALLOC_FAILED;
    unsigned i;
    unsigned numPages = ixfileHandle.fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (ixfileHandle.fileHandle.readPage(i, pageData))
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
// cout<<"current page: "<<currentPage<<endl;
    // parent = currentPage;
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
// cout<<"currentPages entryNumber: "<<entryNumber<<endl;
        //decide where to go
        NodeEntry entry = getNodeEntry(pageData, entryNumber);
// cout<<"currentPages first entry's key: "<<entry.key.intValue<<endl;
        int result = compare(attribute,entry, key);
        //set current page
        // parent = currentPage;
        if(result > 0){
            currentPage = entry.leftChildPageNum;
// cout<<"going to left child"<<endl;
        }else{
            currentPage = entry.rightChildPageNum;
// cout<<"going to right child: "<<currentPage<<endl;
// cout<<"entry key: "<<entry.key.intValue<<endl;
// cout<<"entry rid: "<<entry.rid.slotNum<<endl;
        }
        free(pageData);
    }
    return currentPage;
}

unsigned IndexManager::findPointerEntry(void* page, const Attribute &attribute, const void *key){
    NodeHeader nodeHeader = getNodePageHeader(page);
    unsigned i;
    for(i=0; i<nodeHeader.indexEntryNumber; i++){
        NodeEntry entry = getNodeEntry(page, i);
        int result = compare(attribute, entry, key);
// cout<<"result: "<<result<<endl;
        if(result > 0 || i==nodeHeader.indexEntryNumber-1){
            break;
        }
    }
    return i;
}

//returns -1 if entry.key is less than key, returns 1 otherwise
//sign points at whatever is smaller
int IndexManager::compare(const Attribute &attribute, NodeEntry &entry, const void *key){
    if(attribute.type == TypeVarChar){
        int lengthOfVarChar;
        memcpy(&lengthOfVarChar,key, sizeof(int));
        char varCharBuffer[lengthOfVarChar+1];
        memcpy(varCharBuffer,(char*)key+sizeof(int), lengthOfVarChar);
        varCharBuffer[lengthOfVarChar]='\0';

        return strcmp(entry.key.strValue, varCharBuffer);
    }else if(attribute.type == TypeReal){
        if(entry.key.floatValue<*((float*)key)){return -1;}
        else if(entry.key.floatValue==*((float*)key)){return 0;}
        else {return 1;}                            
    }else{
// cout<<"entry value: "<<entry.key.intValue<<" key: "<<*((int*)key)<<endl;
        if(entry.key.intValue<*((int*)key)){ return -1;}     
        else if(entry.key.intValue == *((int*)key)){return 0;}
        else {return 1;}
    }
}

void IndexManager::setEntryAtOffset(void* page, unsigned offset, NodeEntry &entry){
    NodeHeader nodeHeader = getNodePageHeader(page);
// cout<<"end of entries: "<<nodeHeader.endOfEntries<<" offset: "<<offset<<endl;
    int movingBlockSize = nodeHeader.endOfEntries - offset;
// cout<<"movingblocksize: "<<movingBlockSize<<endl;
    memmove((char*)page+offset+sizeof(NodeEntry), (char*)page+offset, movingBlockSize);
    
    memcpy((char*)page + offset, &entry, sizeof(NodeEntry));
}

void IndexManager::insertInSortedOrder(void* page, const Attribute &attribute, const void *key, const RID &rid, unsigned left, unsigned right){
    //insert in sorted order
    //find correct place
    NodeHeader nodeHeader = getNodePageHeader(page);
    unsigned entryNum = findPointerEntry(page, attribute, key);
    NodeEntry testEntry = getNodeEntry(page, entryNum);
    if(compare(attribute, testEntry, key)<0  && nodeHeader.indexEntryNumber !=0) entryNum++;
// cout<<"entryNum: "<<entryNum<<endl;
    unsigned offset = sizeof(NodeHeader) + entryNum * sizeof(NodeEntry);
    //insert entry
    NodeEntry entry;
    entry.rid = rid;
    if(attribute.type == TypeInt){
        entry.key.intValue = *((int*)key);               
    }else if(attribute.type == TypeReal){
        entry.key.floatValue = *((float*)key);
    }else{
        int lengthOfVarChar;
        memcpy(&lengthOfVarChar,key, sizeof(int));
        char varCharBuffer[lengthOfVarChar+1];
        memcpy(varCharBuffer,(char*)key+sizeof(int), lengthOfVarChar);
        varCharBuffer[lengthOfVarChar]='\0';
        entry.key.strValue = varCharBuffer;           //may be wrong  -----------------------
    }
    entry.leftChildPageNum = left;
    entry.rightChildPageNum = right;
    setEntryAtOffset(page, offset, entry);
    //update node page header
    nodeHeader.indexEntryNumber++;
    nodeHeader.endOfEntries+=sizeof(NodeEntry);
    setNodePageHeader(page, nodeHeader);
    // ixfileHandle.fileHandle.writePage(pageNum,pageData);
}

unsigned IndexManager::splitPage(IXFileHandle &ixfileHandle, void* page, unsigned currentPageNum, unsigned parent, const Attribute &attribute, const void *key, const RID &rid){
    NodeHeader nodeHeader = getNodePageHeader(page);
    //get mid of old page
    unsigned mid = floor((double) nodeHeader.indexEntryNumber / 2);
    unsigned midOffset = sizeof(NodeHeader) + mid * sizeof(NodeEntry);
    NodeEntry midEntry = getNodeEntry(page, mid);
    //make new page
    void * newPageData = calloc(PAGE_SIZE, 1);
    if (newPageData == NULL)
        return IX_MALLOC_FAILED;
    newIndexPage(newPageData);

    //redistribute entries
    NodeHeader newNodeHeader = getNodePageHeader(newPageData);
    if(nodeHeader.isLeaf == true){
        newNodeHeader.isLeaf = true;
        memcpy((char*)newPageData+sizeof(NodeHeader), (char*)page+midOffset, nodeHeader.endOfEntries - midOffset);
        newNodeHeader.endOfEntries = sizeof(NodeHeader) + (nodeHeader.endOfEntries-midOffset);
        if(nodeHeader.indexEntryNumber%2==0){
            newNodeHeader.indexEntryNumber = nodeHeader.indexEntryNumber - mid;
        }else{ 
            newNodeHeader.indexEntryNumber = nodeHeader.indexEntryNumber - mid +1;
        }
    }else{
        memcpy((char*)newPageData+sizeof(NodeHeader), (char*)page+midOffset+sizeof(NodeEntry), nodeHeader.endOfEntries - midOffset - sizeof(NodeEntry));
        newNodeHeader.endOfEntries = sizeof(NodeHeader) + (nodeHeader.endOfEntries-midOffset- sizeof(NodeEntry));
        if(nodeHeader.indexEntryNumber %2 == 0) {
            newNodeHeader.indexEntryNumber = nodeHeader.indexEntryNumber - mid-1;
        }else {
            newNodeHeader.indexEntryNumber = nodeHeader.indexEntryNumber - mid;
        }
    }
    // if(nodeHeader.indexEntryNumber%2==0){        //check============================
        nodeHeader.endOfEntries = midOffset;
        nodeHeader.indexEntryNumber = mid;
    // }else{
    //     nodeHeader.endOfEntries = midOffset;
    //     nodeHeader.indexEntryNumber = mid;
    // }
    //set them as chain if leaves
    //if exists update the node to the right of the old header
    unsigned newNodePageNum = ixfileHandle.fileHandle.getNumberOfPages();
    if(nodeHeader.rightPageNum >-1 ){
        void* rightPageData = malloc(PAGE_SIZE);
        if (ixfileHandle.fileHandle.readPage(nodeHeader.rightPageNum, rightPageData))
            return IX_READ_FAILED;
        NodeHeader rightNodeHeader = getNodePageHeader(rightPageData);
        rightNodeHeader.leftPageNum = newNodePageNum;
        setNodePageHeader(rightPageData, rightNodeHeader);
        if(ixfileHandle.fileHandle.writePage(nodeHeader.rightPageNum, rightPageData))
            return IX_WRITE_FAILED;
    }
    newNodeHeader.rightPageNum = nodeHeader.rightPageNum;
    //set headers
    nodeHeader.rightPageNum = newNodePageNum;
    newNodeHeader.leftPageNum = currentPageNum;
    //create new page if old node is root 

    if(nodeHeader.isRoot == true){
    // if(currentPageNum.isRoot == true){
        nodeHeader.parent = newNodePageNum +1;
        newNodeHeader.parent = newNodePageNum +1;
        //make new page
        void * newRootPage = malloc(PAGE_SIZE);
        newIndexPage(newRootPage);
        nodeHeader.isRoot = false;
        if(attribute.type == TypeVarChar){
            insertInSortedOrder(newRootPage, attribute, midEntry.key.strValue, midEntry.rid, currentPageNum, newNodePageNum);
        }else if(attribute.type == TypeReal){
            insertInSortedOrder(newRootPage, attribute, &(midEntry.key.floatValue), midEntry.rid, currentPageNum, newNodePageNum);
        }else{
            insertInSortedOrder(newRootPage, attribute, &(midEntry.key.intValue), midEntry.rid, currentPageNum, newNodePageNum);
        }
        NodeHeader newRootHeader = getNodePageHeader(newRootPage);
        newRootHeader.isRoot = true;  
        // ixfileHandle.rootPage = newNodePageNum +1;
        setNodePageHeader(newRootPage,newRootHeader);
        setNodePageHeader(page, nodeHeader);
        setNodePageHeader(newPageData, newNodeHeader);
        int result = compare(attribute, midEntry, key);
        if(nodeHeader.isLeaf==true){
            if(result < 0){
                insertInSortedOrder(newPageData,attribute,key,rid,-1,-1);
            }else{
                insertInSortedOrder(page,attribute,key,rid,-1,-1);
            }
        }else{
            if(result < 0){
                insertInSortedOrder(newPageData,attribute,key,rid,currentPageNum,newNodePageNum);
            }else{
                insertInSortedOrder(page,attribute,key,rid,currentPageNum,newNodePageNum);
            }
        }
        //write pages 
        if(ixfileHandle.fileHandle.writePage(currentPageNum, page))
            return IX_WRITE_FAILED;
        if(ixfileHandle.fileHandle.appendPage(newPageData))
            return IX_APPEND_FAILED;
        if(ixfileHandle.fileHandle.appendPage(newRootPage)){
            // free(newRootPage);
            return IX_APPEND_FAILED;
        }
    }else{
        newNodeHeader.parent = nodeHeader.parent;
        setNodePageHeader(page, nodeHeader);
        setNodePageHeader(newPageData, newNodeHeader);
        int result = compare(attribute, midEntry, key);
        if(nodeHeader.isLeaf==true){
            if(result < 0){
                insertInSortedOrder(newPageData,attribute,key,rid,-1,-1);
            }else{
                insertInSortedOrder(page,attribute,key,rid,-1,-1);
            }
        }else{
            if(result < 0){
                insertInSortedOrder(newPageData,attribute,key,rid,currentPageNum,newNodePageNum);
            }else{
                insertInSortedOrder(page,attribute,key,rid,currentPageNum,newNodePageNum);
            }
        }
    //write pages 
        void* parentPageData = malloc(PAGE_SIZE);
        if(ixfileHandle.fileHandle.writePage(currentPageNum, page))
            return IX_WRITE_FAILED;
        if(ixfileHandle.fileHandle.appendPage(newPageData))
            return IX_APPEND_FAILED;
        if (ixfileHandle.fileHandle.readPage(parent, parentPageData) != SUCCESS)
        {
            free(parentPageData);
            return IX_READ_FAILED;
        }
        NodeHeader parentHeader = getNodePageHeader(parentPageData);
        if(parentHeader.endOfEntries+sizeof(NodeEntry)>PAGE_SIZE){
            splitPage(ixfileHandle, parentPageData, parent, parentHeader.parent,attribute, key, rid);
        }else{
            if(attribute.type == TypeVarChar){
                insertInSortedOrder(parentPageData, attribute, midEntry.key.strValue, midEntry.rid, currentPageNum, newNodePageNum);
            }else if(attribute.type == TypeReal){
                insertInSortedOrder(parentPageData, attribute, &(midEntry.key.floatValue), midEntry.rid, currentPageNum, newNodePageNum);
            }else{
                insertInSortedOrder(parentPageData, attribute, &(midEntry.key.intValue), midEntry.rid, currentPageNum, newNodePageNum);
            }
            if(ixfileHandle.fileHandle.writePage(parent, parentPageData)){
                return IX_WRITE_FAILED;
            }
        }
    }
}