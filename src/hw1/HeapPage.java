package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapPage {

	//vars
	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;


	//constructor
	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
			
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		
		dis.close();
	}

	public int getId() {		
		return this.id;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	
	//not using partial ones
	//slot size = tuple size
	//each slot takes 1 bit (to indicate if it is occupied) in header
	public int getNumSlots() {
		int numSlots = HeapFile.PAGE_SIZE*8 / (this.td.getSize()*8 + 1);
		return numSlots / 8;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {     
		
		int headerSize = (int) Math.ceil (HeapFile.PAGE_SIZE*8 / (this.td.getSize()*8 + 1));

		return headerSize;
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 */
	
	
	//bit wise and and or
	public boolean slotOccupied(int s) {
		final BitSet set = BitSet.valueOf(header);
		
		return set.get(s);
		
		
//		//your code here
//		System.out.println(s);
//		System.out.println(this.header);
//		
//		//bitNSet = (byteValue & (1 << N) == 1 << N)
//		//39 AND 4:
//		//100111 AND 
//		//000100 =
//		//------
//		//000100
//		//if q AND n is equal to n, then bit n was set (to 1).
//		
//		//shifting 1 left by N will give us a binary pattern where only the Nth bit is set.
//		//notes from https://www.codeproject.com/Questions/41170/How-do-you-find-value-of-bits-in-byte
//		
//		//index from header byte array
//		int bytePos = s/8;
//		
//		//relative bit position in a byte
//		int bitPosInByte = bytePos % 8;
//		//in the byte move the bit to position 0
//	    return (this.header[bytePos] >> bitPosInByte & 1) == 1;

	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		final BitSet set = BitSet.valueOf(header);
		if (value == true) {
			set.set(s, true);
		}
		else {
			set.set(s, false);
		}
		
		
		
//		int bytePos = s/8;
//		int bitPosInByte = bytePos % 8;		
//		//try to set if unoccupied
//		if (!slotOccupied(s)){
//			set.set(s);
////			this.header[bytePos] >> bitPosInByte & 1 = 0;
////			this.header[bytePos] = this.header[bytePos] | (1 << bitPosInByte);
//			//get current bit
////			(this.header[bytePos] >> s) & 1 = 1;
//			
//		}
//		else {
//			
//		}

		//set to 1
	}
	
	
	
//	custom func
	public void getAvailableSlot() {
		
	}
	
	
	
	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		
		final BitSet set = BitSet.valueOf(header);

		try {
		
			for (int i =0; i <set.length(); i++) {
				//add if not occupied
				if (!this.slotOccupied(i)) {
					this.tuples[i] = t;
					//set ith slot as occupied
					this.setSlotOccupied(i, true);
				}
			}
		
			
		} catch (Exception e){
			throw e;
		}
		//your code here
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		final BitSet set = BitSet.valueOf(header);

		try {
			
			if (t.getPid() != this.getId()) {
				throw new Exception("not same page");
			}
			else {
				for (int i =0; i <set.length(); i++) {
					//delete if occupied
					if (this.slotOccupied(i)) {
						this.tuples[i] = null;
						//set ith slot as unoccupied
						this.setSlotOccupied(i, false);
					}
					else {
						throw new Exception("the slot is already empty");
					}
				}
			}
		
		} catch (Exception e){
			throw e;
		}
		//your code here
	}
		
			
	
	/**
     * Suck up tuples from the source file.
     */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
	 *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
		
		//create arraylist out of this.tuples so have iterator
        final List<Tuple> tupleList = new ArrayList<Tuple>(Arrays.asList(this.tuples));
		return tupleList.iterator();
	}
}
