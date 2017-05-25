#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <iostream>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

# define  IXF_COLLECT_VALUES_FAIL 1
# define  IX_MALLOC_FAILED 2
# define  IX_OPEN_FAILED 3
# define  IX_APPEND_FAILED 4
# define  IX_CREATE_FAILED 5
# define  IX_READ_FAILED 6
# define  IX_WRITE_FAILED 7
# define  IX_DELETION_DNE 8
# define  IX_FILE_DNE 9

class IX_ScanIterator;
class IXFileHandle;

typedef struct NodeHeader      //page header
{
    uint16_t endOfEntries;
    uint16_t indexEntryNumber;
    bool isLeaf;
    // bool isRoot;
    int16_t leftPageNum;
    int16_t rightPageNum;
    int16_t parent;
} NodeHeader;

typedef struct NodeEntry {
    RID rid;
    //need key value
    union{
        int intValue;
        float floatValue;
        char* strValue;
    }key;
    int16_t leftChildPageNum;
    int16_t rightChildPageNum;
} NodeEntry;

class IndexManager {

    public:
        static IndexManager* instance();
        static PagedFileManager *_pf_manager;

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        friend class IX_ScanIterator;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;

        void newIndexPage(void * page);     //creates a new Index Page

        NodeHeader getNodePageHeader(void * page)const;      //returns the node page header
        void setNodePageHeader(void * page, NodeHeader nodeHeader);     //sets the node page header

        NodeEntry getNodeEntry(void* page, unsigned entryNum)const;   //returns the node entry on the page corresponding to the pageNum
        unsigned getRootPageNum(IXFileHandle ixfileHandle)const;     //returns the page number of the root of the tree

        unsigned traverse(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key); //finds the correct leaf based on the key
        unsigned findPointerEntry(void* page, const Attribute &attribute, const void *key);     //returns the entry number of the first entry with a key not
                                                                                                //less than the specified key
        unsigned findPointerEntryInParent(void* page, const Attribute &attribute, const void *key);

        int compare(const Attribute &attribute, NodeEntry &entry, const void *key);      //returns -1 if entry.key is less, 1 otherwise
        int compareInParent(const Attribute &attribute, NodeEntry &entry, const void *key);

        void setEntryAtOffset(void* page, unsigned offset, NodeEntry &entry);        //moves all the entries after the entry and inserts the entry at the offset
                                                                                    //does not update the page header
        void insertInSortedOrder(void* page,const Attribute &attribute, const void *key, const RID &rid, unsigned left, unsigned right);
        void insertInParent(void* page, const Attribute &attribute, const void *key, const RID &rid, unsigned left, unsigned right);

        unsigned splitPage(IXFileHandle &ixfileHandle, void* page, unsigned currentPageNum, 
                unsigned parent, const Attribute &attribute, const void *key,const RID &rid);

        void printRecursively(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageNum, unsigned tabs)const;
    
        int getDeletionSlotNum(void* page, const Attribute &attribute, const void* key);
        void deleteEntryAtOffset(void* page, unsigned offset);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        friend class IndexManager;
    private:
        IndexManager *_ix_manager;
        int currentPage;
        unsigned currentEntry;
        unsigned maxPage;
        unsigned maxEntry;
        IXFileHandle *ixfileHandle;
        Attribute attribute;
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    
    int rootPage;
    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class IndexManager;
    friend class IX_ScanIterator;
    private:

    FileHandle fileHandle;

};

#endif
