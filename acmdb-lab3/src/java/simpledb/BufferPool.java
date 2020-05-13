package simpledb;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
// import java.util.NoSuchElementException;
// import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

    // private ConcurrentHashMap<Integer, PageId> BufferPoolPageID = null;
    // private ConcurrentHashMap<PageId, Integer> indexInList = null;
    // private ConcurrentHashMap<PageId, Page> VictimCache = null;
    // private int FirstID = 0;
    private Random rand = null;

    private PidTimeStamp NCache,VCache;

    private int cnt = 0;
    
    /**
     * Evict the next page
     * Using LRU
     * @throws IOException
     */
    private synchronized void EvictNext()throws IOException{
        // if (1==1)throw new NotImplementedException();
        PageId EvPageID = NCache.getFirst();
        if (EvPageID!=null)
        try_Evict(EvPageID);
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
        // rand = new Random();
        VCache = new PidTimeStamp();
        NCache = new PidTimeStamp();

        // BufferPoolPageID = new ConcurrentHashMap<>();
        // indexInList = new ConcurrentHashMap<>();
        // VictimCache = new ConcurrentHashMap<>();
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
        // if (true)throw new NotImplementedException();
        // some code goes here
        ++cnt;
        if (pid == null) throw new DbException("pid is null");
        Page ans = Buffer_Pool_RAM.get(pid);
        if (ans!=null) {
            NCache.insert(pid, cnt);
            VCache.remove(pid);
            return ans;
        }
        // System.out.println(Buffer_Pool_RAM.size()+", "+this.numOfPages+", "+NCache.size());
        // new page
        if (Buffer_Pool_RAM.size()>=this.numOfPages&&NCache.size()>0){
            // System.out.println(Buffer_Pool_RAM.size()+", "+this.numOfPages);
            while (Buffer_Pool_RAM.size()>=this.numOfPages&&NCache.size()>0){
                try{
                    EvictNext();
                } catch(IOException e){
                    throw new NotImplementedException();
                }
            }
        }
        ans = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        Buffer_Pool_RAM.put(pid, ans);
        NCache.insert(pid, cnt);
        VCache.remove(pid);
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
        // throw new NotImplementedException();
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // throw new NotImplementedException();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // throw new NotImplementedException();
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
        // throw new NotImplementedException();
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
        //if (Database.getCatalog().getDatabaseFile(tableId).getClass()==BTreeFile.class){
        if (Database.getCatalog().getDatabaseFile(tableId) instanceof DbFile){
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            // Field field = t.getField(file.keyField());
            ArrayList<Page> dpage = 
            file.insertTuple(tid, t);
            for (PageId pageid: VCache.PageIdVector()){
                //TODO: flush?  Please Check the lock before writeback
                Page page = Buffer_Pool_RAM.get(pageid);
                if (VCache.getTime(page.getId())!=-1){
                    file.writePage(page);
                    discardPage(page.getId());
                }
            }
            for (Page page:dpage){
                if (NCache.getTime(page.getId())==-1){
                    // System.out.println("Not Defeat Method to use BufferPool 1");
                    file.writePage(page);
                }
            }
            return;
        } else{
            throw new NotImplementedException();
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
        // t.getRecordId().getPageId();
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        if (file instanceof DbFile){
            ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
            // for (Page page: dirtyPages){
            for (PageId pageid: VCache.PageIdVector()){
                //TODO: flush? 
                Page page = Buffer_Pool_RAM.get(pageid);
                if (VCache.getTime(page.getId())!=-1){
                    file.writePage(page);
                    discardPage(page.getId());
                }
            }
            for (Page page:dirtyPages){
                if (NCache.getTime(page.getId())==-1){
                    // System.out.println("Not Defeat Method to use BufferPool 1");
                    file.writePage(page);
                }
            }

        } else {
            throw new NotImplementedException();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        // System.out.println("flushAll");
        ++cnt;
        Iterator<PageId> iter;
        iter = NCache.PageIdVector().iterator();
        while (iter.hasNext()){
            PageId pid = iter.next();
            flushPage(pid);
        }
        iter = VCache.PageIdVector().iterator();
        while (iter.hasNext()){
            PageId pid = iter.next();
            flushPage(pid);
        }
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
        if (pid==null) return;
        {
            Page page = this.Buffer_Pool_RAM.get(pid);
            if (page!=null){
                this.Buffer_Pool_RAM.remove(pid);
                NCache.remove(pid);
                VCache.remove(pid);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        ++cnt;
        Page page = this.Buffer_Pool_RAM.get(pid);
        if (page == null) {
            throw new IOException("Pid Not Found");
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (page.isDirty()!=null){
            file.writePage(page);
            page.markDirty(false, null);
            VCache.remove(pid);
            NCache.insert(pid, cnt);
            return;
        }else{
            // file.writePage(page);
            // VCache.remove(pid);
            // NCache.insert(pid, cnt);
            return;
        }
    }
    /**
     * Evict a certain page from Normal Cache to Victim Cache if Dirty
     * Remove if clean
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void try_Evict(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // TODO: Try to open the lock to writeback
        ++cnt;
        Page page = this.Buffer_Pool_RAM.get(pid);
        if (page == null) {
            throw new IOException("Pid Not Found");
        }
        // DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (page.isDirty()==null){
            // flushPage(pid);
            discardPage(pid);
        }else{
            NCache.remove(pid);
            VCache.insert(pid, cnt);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        throw new NotImplementedException();

    }
    public synchronized boolean hasPage(PageId pid){
        // System.out.println(Buffer_Pool_RAM);
        if (this.Buffer_Pool_RAM.containsKey(pid))
        return true;
        else return false;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        throw new NotImplementedException();
    }
    
    private class PidTimeStamp{
        private TreeMap<Integer,PageId> getid;
        private ConcurrentHashMap<PageId,Integer> gettime;
        public PidTimeStamp(){
            getid = new TreeMap<>();
            gettime = new ConcurrentHashMap<>();
            // System.err.println("PidTImeStamp");
        }
        public synchronized void insert(PageId pid, int timeStamp){
            if (gettime.containsKey(pid)){
                int lastTime = gettime.get(pid);
                getid.remove(lastTime);
                gettime.remove(pid);
            }
            getid.put(timeStamp, pid);
            gettime.put(pid, timeStamp);
        }
        public synchronized int getTime(PageId pid){
            if (pid==null) return -1;
            if (!gettime.containsKey(pid)) return -1;
            return gettime.get(pid);
        }
        public synchronized PageId getPid(int timeStamp){
            return getid.get(timeStamp);
        }
        public synchronized void remove(PageId pid){
            if (pid==null) return;
            if (!gettime.containsKey(pid)) return;
            int lastTime = gettime.get(pid);
            gettime.remove(pid);
            getid.remove(lastTime);
        }
        public synchronized PageId getFirst(){
            if (getid.size()==0) return null;
            return getid.firstEntry().getValue();
        }
        public synchronized int size(){
            return gettime.size();
        }
        public synchronized Iterator<PageId> iterator(){
            return gettime.keySet().iterator();
        }
        public synchronized Vector<PageId> PageIdVector(){
            // Set<PageId> set = gettime.keySet();
            Vector<PageId> ans = new Vector<>();
            Iterator<PageId> iter = this.iterator();
            while (iter.hasNext()){
                ans.add(iter.next());
            }
            return ans;
        }
        // public synchronized void clear(){
        //     gettime.clear();
        //     getid.clear();
        // }
    }
}