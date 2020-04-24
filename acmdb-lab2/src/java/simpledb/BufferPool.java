package simpledb;

import java.io.*;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.Vector;
// import java.util.NoSuchElementException;
// import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;
    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public int numOfPages = DEFAULT_PAGES;

    // private Vector<Page> Buffer_Pool_RAM = null;
    private ConcurrentHashMap<PageId, Page> Buffer_Pool_RAM = null;
    private ConcurrentHashMap<Integer, PageId> BufferPoolPageID = null;
    private ConcurrentHashMap<PageId, Integer> indexInList = null;
    private int FirstID = 0;
    private Random rand = null;

    private int cnt = 0;

    /**
     * Get Evict Index and Remove from LRU List
     * @return The Next Evicted PageID
     */
    private synchronized PageId decideNextEviction(){
        PageId ans = BufferPoolPageID.get(FirstID);
        while (ans==null){
            ++FirstID;
            ans = BufferPoolPageID.get(FirstID);
        }
        BufferPoolPageID.remove(FirstID);
        indexInList.remove(ans);
        return ans;
    }
    
    /**
     * Evict the next page
     * Using LRU
     * @throws IOException
     */
    private synchronized void EvictNext()throws IOException{
        PageId EvPageID = decideNextEviction();
        flushPage(EvPageID);
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        numOfPages = numPages;
        Buffer_Pool_RAM = new ConcurrentHashMap<>();
        rand = new Random();
        BufferPoolPageID = new ConcurrentHashMap<>();
        indexInList = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException{
        // some code goes here
        ++cnt;
        if (pid == null) throw new DbException("pid is null");
        Page ans = Buffer_Pool_RAM.get(pid);
        if (ans!=null) {
            int index = indexInList.get(pid);
            indexInList.remove(pid);
            indexInList.put(pid, cnt);
            BufferPoolPageID.remove(index);
            BufferPoolPageID.put(cnt, pid);
            return ans;
        }
        // new page
        // System.out.println("Notice! by Jiasen ");
        // Random rnd = new Random();
        // int evict = java.util.Random;
        while (Buffer_Pool_RAM.size()>=this.numOfPages){
            try{
                EvictNext();
            } catch(IOException e){
                throw new NotImplementedException();
            }
        }
        ans = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        Buffer_Pool_RAM.put(pid, ans);
        // System.out.println(pid);
        // System.out.println(cnt);
        indexInList.put(pid, cnt);
        this.BufferPoolPageID.put(cnt, pid);
        return ans;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HashMap<PageId,Page> dirtypages = new HashMap<>();
        if (Database.getCatalog().getDatabaseFile(tableId).getClass()==BTreeFile.class){
            BTreeFile file = (BTreeFile)Database.getCatalog().getDatabaseFile(tableId);
            Field field = t.getField(file.keyField());
            BTreeLeafPage leaf = file.findLeafPage(tid, 
                file.getRootPtrPage(tid, dirtypages).getRootId(),
                Permissions.READ_WRITE, field);
            // System.out.println(leaf.View()+" + "+t);
            leaf = file.splitLeafPage(tid, dirtypages, leaf, field);
            // if (leaf.iterator().hasNext()){
            //     if (!(leaf.iterator().next().getField(file.keyField()).compare(Predicate.Op.LESS_THAN_OR_EQ, field))){
            //         throw new NotImplementedException();
            //     }
            //     if (!(leaf.reverseIterator().next().getField(file.keyField()).compare(Predicate.Op.GREATER_THAN_OR_EQ, field))){
            //         throw new NotImplementedException();
            //     }
            // }
            leaf.insertTuple(t);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.Buffer_Pool_RAM.get(pid);
        if (page == null) {
            throw new IOException("Pid Not Found");
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (page.isDirty()!=null){
            file.writePage(page);
            throw new NotImplementedException();
        }
        this.Buffer_Pool_RAM.remove(pid);
        return;
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

}
