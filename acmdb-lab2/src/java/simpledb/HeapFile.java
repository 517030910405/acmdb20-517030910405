package simpledb;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.*;

import org.omg.CORBA.portable.UnknownException;



// import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private RandomAccessFile rfile;
    private int tableid;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        tableid = file.getAbsoluteFile().hashCode();
        try{
            rfile = new RandomAccessFile((File)file, (String)"rw");
        } catch (FileNotFoundException e){
            
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableid;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // Page ans = new HeapPage((HeapPageId)pid,new byte[255]);
        
        int pageNum = pid.pageNumber();
        int pageSize = BufferPool.getPageSize();
        byte data[] = new byte[pageSize];
        try{
            // rfile.read(data, (pageNum)*pageSize, pageSize);
            rfile.seek(((long)pageNum)*pageSize); 
            rfile.read(data);
            Page newPage = new HeapPage((HeapPageId)pid, data);
            return newPage;
        } catch(IOException e){
            // throw e;
        }
        // file.getAbsoluteFile().
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        byte[] data = page.getPageData();
        rfile.seek(((long)pid.pageNumber())*BufferPool.getPageSize());
        rfile.write(data);
        // throw new AssertionError();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try{
            return (int)(rfile.length()/(long)BufferPool.getPageSize());
        } catch(IOException e){
            throw new RuntimeException("HeapFile.numPages Exception by Jiasen");
        }
        // return 0;
    }

    // see #DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> ans = new ArrayList<>();
        HashMap<PageId,Page> dirtypages = new HashMap<>();
        int numOfPages = this.numPages();
        for (int i=0;i<numOfPages;++i){
            Page page = this.getPage(tid, dirtypages, 
                new HeapPageId(this.getId(), i), Permissions.READ_ONLY);
            HeapPage hpage = (HeapPage) page;
            if (hpage.getNumEmptySlots()>0){
                page = this.getPage(tid, dirtypages, 
                    new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
                hpage = (HeapPage) page;
                hpage.insertTuple(t);
                ans.addAll(dirtypages.values());
                return ans;
            }
        }
        HeapPageId pid = new HeapPageId(this.getId(), numOfPages);
        HeapPage page = new HeapPage(pid,
            HeapPage.createEmptyPageData());
        page.insertTuple(t);
        this.writePage(page);
        page = (HeapPage)this.getPage(tid, dirtypages, pid, Permissions.READ_ONLY);
        if (page.getNumEmptySlots()!=page.numSlots-1){
            throw new AssertionError("file open and close error by lijiasen");
        }
        ans.addAll(dirtypages.values());
        return ans;
        // throw new AssertionError();
        // return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // throw new AssertionError();
        ArrayList<Page> ans = new ArrayList<>();
        HashMap<PageId,Page> dirtypages = new HashMap<>();
        Page page = this.getPage(tid, dirtypages, 
            t.getRecordId().getPageId(), Permissions.READ_WRITE);
        HeapPage hpage = (HeapPage) page;
        hpage.deleteTuple(t);
        ans.addAll(dirtypages.values());
        return ans;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator(){
            int PageIndex = -1;
            int TableId = -1;
            Iterator<Tuple> pageTupleIterator = null;

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                PageIndex = 0;
                TableId = getId();
                pageTupleIterator = ( (HeapPage)Database.getBufferPool().
                    getPage(tid, new HeapPageId(TableId, PageIndex), null) ).iterator();              
            }
        
            @Override
            public void open() throws DbException, TransactionAbortedException {
                PageIndex = 0;
                TableId = getId();
                pageTupleIterator = ( (HeapPage)Database.getBufferPool().
                    getPage(tid, new HeapPageId(TableId, PageIndex), null) ).iterator();
            }
        
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return pageTupleIterator.next();
            }
        
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                //Not Opened
                if (pageTupleIterator == null) return false;
                //Opened
                if (pageTupleIterator.hasNext()){
                    return true;
                } else{
                    int num_Pages = numPages();
                    for (int i=PageIndex+1;i<num_Pages;++i){
                        pageTupleIterator = ( (HeapPage)Database.getBufferPool().
                        getPage(tid, new HeapPageId(TableId, i), null) ).iterator();
                        if (pageTupleIterator.hasNext()){
                            PageIndex = i;
                            return true;
                        }
                    }
                    return false;
                }
            }
        
            @Override
            public void close() {
                pageTupleIterator = null;
                PageIndex = -1;
                TableId = -1;
            }
        };
        // return null;
    }
	/**
	 * Method to encapsulate the process of locking/fetching a page.  First the method checks the local 
	 * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.  
	 * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since 
	 * presumably they will soon be dirtied by this transaction.
	 * 
	 * This method is needed to ensure that page updates are not lost if the same pages are
	 * accessed multiple times.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	Page getPage(TransactionId tid, HashMap<PageId, Page> dirtypages, PageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {
		if(dirtypages.containsKey(pid)) {
			return dirtypages.get(pid);
		}
		else {
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if(perm == Permissions.READ_WRITE) {
                p.markDirty(true, tid);
				dirtypages.put(pid, p);
			}
			return p;
		}
	}

}

