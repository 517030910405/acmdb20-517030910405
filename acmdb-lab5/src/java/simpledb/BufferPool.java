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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    public static final int DEFAULT_PAGES = 1000;
    public int numOfPages = DEFAULT_PAGES;

    // private Vector<Page> Buffer_Pool_RAM = null;
    private ConcurrentHashMap<PageId, Page> Buffer_Pool_RAM = null;
    public ConcurrentHashMap<PageId, LockWithTransaction> LockPool = null;
    public ConcurrentHashMap<TransactionId, PageId> waitForPage = null;
    // public ConcurrentHashMap<PageId, HashSet<TransactionId> > LockCount = null;
    // private ConcurrentHashMap<Integer, PageId> BufferPoolPageID = null;
    // private ConcurrentHashMap<PageId, Integer> indexInList = null;
    // private ConcurrentHashMap<PageId, Page> VictimCache = null;
    // private int FirstID = 0;
    private Random rand = null;

    private PidTimeStamp NCache,VCache;

    private int cnt = 0;

    private Object OperatePageLock = new Object();
    private Object atomicOpLock = new Object();
    private Object increaseCNT = new Object();
    // private Object OperateLockLock = new Object();
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
        LockPool = new ConcurrentHashMap<>();
        waitForPage = new ConcurrentHashMap<>();
        // java.util.concurrent.locks.ReentrantReadWriteLock;
        // ReadWriteLock lc =  new ReentrantReadWriteLock();
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
     * Jiasen Method</p>
     * Check there is a lock for pid </p>
     * Atomic Function
     * @param pid
     */
    public void checkLockIsThere(PageId pid){
        synchronized (this.atomicOpLock){
            if (this.LockPool.get(pid)==null){
                this.LockPool.put(pid, new LockWithTransaction(pid));
            }
        }
    }

    /**
     * Jiasen Method</p>
     * get lock
     * @param tid
     * @param pid
     */
    public void getWriteLock(TransactionId tid, PageId pid)throws TransactionAbortedException{
        checkLockIsThere(pid);
        this.LockPool.get(pid).writelock(tid);
    }
    
    /**
     * Jiasen Method</p>
     * get lock
     * @param tid
     * @param pid
     */
    public void getReadLock(TransactionId tid, PageId pid)throws TransactionAbortedException{
        checkLockIsThere(pid);
        this.LockPool.get(pid).readlock(tid);
        // LockWithTransaction lockWithTransaction = this.LockPool.get(pid);
        // lockWithTransaction.lock.readLock().lock();
        // lockWithTransaction.tIds.add(tid);
        // lockWithTransaction.isWrite = false;
    }

    /**
     * Jiasen Method</p>
     * return lock
     * @param tid
     * @param pid
     */
    public void returnWriteLock(TransactionId tid, PageId pid){
        checkLockIsThere(pid);
        synchronized(this){
            this.LockPool.get(pid).unlock(tid);
            if (this.VCache.getTime(pid)!=-1){
                int cnt = this.IncreaseCount();
                this.VCache.remove(pid);
                this.NCache.insert(pid, cnt);
            }
        }
        // LockWithTransaction lockWithTransaction = this.LockPool.get(pid);
        // lockWithTransaction.tIds.remove(tid);
        // lockWithTransaction.lock.writeLock().unlock();
    }

    /**
     * Jiasen Method</p>
     * return lock
     * @param tid
     * @param pid
     */
    public void returnReadLock(TransactionId tid, PageId pid){
        checkLockIsThere(pid);
        synchronized(this){
            this.LockPool.get(pid).unlockread(tid);
            if (this.VCache.getTime(pid)!=-1){
                int cnt = this.IncreaseCount();
                this.VCache.remove(pid);
                this.NCache.insert(pid, cnt);
            }
        }
        // LockWithTransaction lockWithTransaction = this.LockPool.get(pid);
        // lockWithTransaction.tIds.remove(tid);
        // lockWithTransaction.lock.readLock().unlock();
    }
    // public boolean checkReadLockAvalible(PageId pid){
    //     checkLockIsThere(pid);
    //     synchronized(this.atomicOpLock){
    //     }
    //     return false;
    // }

    public int IncreaseCount(){
        synchronized(this.increaseCNT){
            ++cnt;
            return cnt;
        }
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException{
        // if (true)throw new NotImplementedException();
        // some code goes here
        if (pid == null) throw new DbException("pid is null");

        if (perm==Permissions.READ_WRITE){
            this.getWriteLock(tid, pid);
        } else if (perm==Permissions.READ_ONLY){
            this.getReadLock(tid, pid);
        } else{
            System.err.println("Lijs: BufferPool Permission Exception"+perm);
            throw new NotImplementedException();
        }
        // ++cnt;
        // synchronized(this.LockPool.get(pid).getPageLock){
        synchronized(this){
            int cnt = this.IncreaseCount();
            Page ans;
        
            ans = Buffer_Pool_RAM.get(pid);
            if (ans!=null) {
                NCache.insert(pid, cnt);
                VCache.remove(pid);
                if(perm == Permissions.READ_WRITE) {
                    ans.markDirty(true, tid);
                }
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
                if (Buffer_Pool_RAM.size()>=Integer.max(this.numOfPages,0)){
                    // System.err.println("Lijs Error: Buffer Pool Exceeding, Size = "+Buffer_Pool_RAM.size());
                    throw new DbException("Lijs Error: Buffer Pool Exceeding");
                }
            }
            ans = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            Buffer_Pool_RAM.put(pid, ans);
            // LockPool.put(pid, new ReentrantReadWriteLock());
            NCache.insert(pid, cnt);
            VCache.remove(pid);
            if(perm == Permissions.READ_WRITE) {
                ans.markDirty(true, tid);
            }
            return ans;
        }
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
        checkLockIsThere(pid);
        // this.LockPool.get(pid).unlock(tid);
        returnWriteLock(tid, pid);
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
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // throw new NotImplementedException();
        return LockPool.get(p).holdsLock(tid);
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
        // Buffer_Pool_RAM.keySet().iterator();
        Vector <PageId> PIDs = new Vector<>();
        for (Iterator<PageId> IterPid = LockPool.keySet().iterator();IterPid.hasNext();){
            PageId pid = IterPid.next();
            PIDs.add(pid);
        }
        for (Iterator<PageId> IterPid = Buffer_Pool_RAM.keySet().iterator();IterPid.hasNext();){
            PageId pid = IterPid.next();
            if (!LockPool.containsKey(pid))PIDs.add(pid);
        }
        for (Iterator<PageId> IterPid = PIDs.iterator();IterPid.hasNext();){
            PageId pid = IterPid.next();
            if (holdsLock(tid, pid)){
                if (Buffer_Pool_RAM.containsKey(pid)&&tid.equals(Buffer_Pool_RAM.get(pid).isDirty())){
                    if (commit){
                        flushPage(pid);
                    } else{
                        discardPage(pid);
                    }
                }
                releasePage(tid, pid);
            }
        }
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
            if (!t.schema.AllEequals(file.getTupleDesc())){
                System.err.println("BufferPool.insertTuple: Lijiasen FieldName Notice");
                System.err.println(t.schema);
                System.err.println(file.getTupleDesc());
            }
            ArrayList<Page> dpage = 
            file.insertTuple(tid, t);
            // for (PageId pageid: VCache.PageIdVector()){
            //     //TODO: flush?  Please Check the lock before writeback
            //     Page page = Buffer_Pool_RAM.get(pageid);
            //     if (VCache.getTime(page.getId())!=-1){
            //         file.writePage(page);
            //         discardPage(page.getId());
            //     }
            // }
            // for (Page page:dpage){
            //     if (NCache.getTime(page.getId())==-1){
            //         // System.out.println("Not Defeat Method to use BufferPool 1");
            //         file.writePage(page);
            //     }
            // }
            for (Page page:dpage){
                if (!Buffer_Pool_RAM.containsKey(page.getId())){
                    // System.out.println("Not Defeat Method to use BufferPool 1");
                    // file.writePage(page);
                    getWriteLock(tid, page.getId());
                    int cnt = IncreaseCount();
                    Buffer_Pool_RAM.put(page.getId(), page);
                    NCache.insert(page.getId(), cnt);
                    page.markDirty(true, tid);

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
            // for (PageId pageid: VCache.PageIdVector()){
            //     //TODO: flush? 
            //     Page page = Buffer_Pool_RAM.get(pageid);
            //     if (VCache.getTime(page.getId())!=-1){
            //         file.writePage(page);
            //         discardPage(page.getId());
            //     }
            // }
            // for (Page page:dirtyPages){
            //     if (NCache.getTime(page.getId())==-1){
            //         // System.out.println("Not Defeat Method to use BufferPool 1");
            //         file.writePage(page);
            //     }
            // }
            for (Page page:dirtyPages){
                if (!Buffer_Pool_RAM.containsKey(page.getId())){
                    // System.out.println("Not Defeat Method to use BufferPool 1");
                    // file.writePage(page);
                    getWriteLock(tid, page.getId());
                    int cnt = IncreaseCount();
                    Buffer_Pool_RAM.put(page.getId(), page);
                    NCache.insert(page.getId(), cnt);
                    page.markDirty(true, tid);
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
        // ++cnt;
        int cnt = this.IncreaseCount();
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
        // ++cnt;
        int cnt = this.IncreaseCount();
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
        // ++cnt;
        int cnt = this.IncreaseCount();
        Page page = this.Buffer_Pool_RAM.get(pid);
        if (page == null) {
            throw new IOException("Pid Not Found");
        }
        // DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (page.isDirty()==null){
        // if (page.isDirty()==null&&LockPool.get(pid).tIds.size()==0){
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
        // throw new NotImplementedException();
        Vector<Page> TidPages = new Vector<>(2);
        TidPages.addAll(this.Buffer_Pool_RAM.values());
        for (Page page : TidPages){
            if (page.isDirty()!=null && page.isDirty().equals(tid)){
                flushPage(page.getId());
            }
        }
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

    /**
     * A very light-weighted Lock implementation</p>
     * by Jiasen</p>
     * The lock can upgrade</p>
     * The same tid cannot lock twice. Once relock, do nothing. </p>
     */
    public class LockWithTransaction{
        public boolean isWrite = false;
        public HashSet<TransactionId> tIds = new HashSet<TransactionId>(4);
        public Object modifying = new Object();
        public Object getPageLock = new Object();
        public PageId pid = null;
        LockWithTransaction(PageId pid){
            // Thread.currentThread()
            this.pid = pid;
        }
        /**
         * get the read lock </p>
         * will wait until it locks </p>
         * do nothing if it holds the write lock
         * @param tid
         */
        public void readlock(TransactionId tid) throws TransactionAbortedException{
            int counter = 0;
            while (true){
                synchronized(modifying){
                    // The first read lock
                    if (tIds.size()==0){
                        tIds.add(tid);
                        isWrite = false;
                        Database.getBufferPool().waitForPage.remove(tid);
                        return;
                    }
                    // Can add a read lock or hold initaially
                    if (tIds.size()>0&&(!isWrite)){
                        if (!tIds.contains(tid)){
                            tIds.add(tid);
                        }
                        Database.getBufferPool().waitForPage.remove(tid);
                        return;
                    }
                    // Holding a write lock
                    if (isWrite&&tIds.contains(tid)){
                        Database.getBufferPool().waitForPage.remove(tid);
                        return;
                    }
                }
                ++counter;
                Database.getBufferPool().waitForPage.put(tid, pid);
                Thread.yield();
                if (check_cycle_from(tid)){
                    throw new TransactionAbortedException();
                }
            }
        }
        /**
         * Get the write lock </p>
         * or Upgrade the lock to write lock
         * @param tid
         */
        public void writelock(TransactionId tid)throws TransactionAbortedException{
            int counter = 0;
            while (true){
                synchronized(modifying){
                    // The first write lock
                    if (tIds.size()==0){
                        tIds.add(tid);
                        isWrite = true;
                        Database.getBufferPool().waitForPage.remove(tid);
                        return;
                    }
                    // Upgrade lock or hold initially
                    if (tIds.size()==1&&tIds.contains(tid)){
                        isWrite = true;
                        Database.getBufferPool().waitForPage.remove(tid);
                        return;
                    }
                }
                ++counter;
                Database.getBufferPool().waitForPage.put(tid, pid);
                Thread.yield();
                if (check_cycle_from(tid)){
                    throw new TransactionAbortedException();
                }
            }
        }
        /**
         * Unlock. Whether it is r/w lock does not matter. 
         * @param tid
         */
        public void unlock(TransactionId tid){
            synchronized(modifying){
                if (!tIds.contains(tid)){
                    System.err.println("DO NOTHING WHEN UNLOCK by JIASEN");
                    // throw new NotImplementedException();
                }
                tIds.remove(tid);
            }
        }
        /**
         * Unlock. Whether it is r/w lock does not matter. 
         * @param tid
         */
        public void unlockread(TransactionId tid){
            synchronized(modifying){
                if (!this.isWrite) tIds.remove(tid);
            }
        }

        public boolean holdsLock(TransactionId tid){
            synchronized(modifying){
                return tIds.contains(tid);
            }
        }
        public boolean check_cycle(TransactionId tid,Vector<TransactionId> TIDs){
            PageId pid;
            pid = Database.getBufferPool().waitForPage.get(tid);
            if (pid!=null){
                LockWithTransaction lockWithTransaction =  Database.getBufferPool().LockPool.get(pid);
                if (lockWithTransaction!=null){
                    Vector<TransactionId> next = new Vector<>();
                    synchronized(lockWithTransaction.modifying){
                        for (Iterator<TransactionId> iter = lockWithTransaction.tIds.iterator();iter.hasNext();){
                            next.add(iter.next());
                        }
                    }
                    for (TransactionId nextTid: next){
                        if (TIDs.contains(nextTid)){
                            return true;
                        } else {
                            TIDs.add(nextTid);
                            if (check_cycle(nextTid, TIDs)){
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }
        public boolean check_cycle_from(TransactionId tid){
            return check_cycle(tid, new Vector<TransactionId>());
        }
    };
}