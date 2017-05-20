#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

# define  IXF_COLLECT_VALUES_FAIL 1
# define  IX_MALLOC_FAILED 2
# define  IX_OPEN_FAILED 3
# define  IX_APPEND_FAILED 4
# define  IX_CREATE_FAILED 5
# define  IX_READ_FAILED 6

class IX_ScanIterator;
class IXFileHandle;

typedef struct NodeHeader      //page header
{
    uint16_t freeSpaceOffset;
    uint16_t indexEntryNumber;
    bool isLeaf;
    bool isRoot;
    uint16_t leftPageNum;
    uint16_t rightPageNum;
} NodeHeader;

typedef struct NodeEntry {
    RID rid;
    //need key value
    uint16_t leftChildPageNum;
    uint16_t rightChildPageNum;
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

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;

        void newIndexPage(void * page);
        NodeHeader getNodePageHeader(void * page);
        void setNodePageHeader(void * page, NodeHeader nodeHeader);
        unsigned getRootPageNum(IXFileHandle ixFileHandle);
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
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class IndexManager;
    private:

    FileHandle fileHandle;

};

#endif
