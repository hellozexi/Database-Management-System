package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

import javax.security.auth.kerberos.KerberosKey;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc desc;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.file = f;
		this.desc = type;
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.desc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] data = new byte[PAGE_SIZE];
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
			randomAccessFile.seek(id * PAGE_SIZE);
			randomAccessFile.read(data);
			randomAccessFile.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HeapPage res = null;
		try {
			res = new HeapPage(id, data, getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return res;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return file.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		byte[] data = p.getPageData();
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
			randomAccessFile.seek(p.getId() * PAGE_SIZE);
			randomAccessFile.write(data);
			randomAccessFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		
		HeapPage heappage = null;
		boolean founded = false;
		byte[] data = new byte[PAGE_SIZE];
		//find a page with open slot
		for (int i = 0; i < this.getNumPages(); i++) {
			HeapPage page = this.readPage(i);
			founded = false;
			if (page.hasAvailableSlot()) {
				heappage = page;
				founded = true;
				break;
			}
		}
		
		//add tuple
		try {
			if(founded) {
				heappage.addTuple(t);
				this.writePage(heappage);
			} else {
				heappage = new HeapPage(this.getNumPages(), data, this.getId());
				heappage.addTuple(t);
				this.writePage(heappage);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return heappage;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		HeapPage heapPage = this.readPage(t.getPid());
		try {
			heapPage.deleteTuple(t);
			writePage(heapPage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> list = new ArrayList<Tuple>();
		for(int i = 0; i < this.getNumPages(); i++) {
			HeapPage curHeapPage = readPage(i);
			Iterator<Tuple> iterator = curHeapPage.iterator();
			while(iterator.hasNext()) {
				list.add(iterator.next());
			}
		}
		return list;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int) Math.ceil(file.length() / PAGE_SIZE);
	}
}
