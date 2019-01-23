package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class YourUnitTests {
	
	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	private HeapPage hp;

//	@Before
//	public void setup() {
//		
//		try {
//			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
//		} catch (IOException e) {
//			System.out.println("unable to copy files");
//			e.printStackTrace();
//		}
//		
//		c = Database.getCatalog();
//		c.loadSchema("testfiles/test.txt");
//		
//		int tableId = c.getTableId("test");
//		td = c.getTupleDesc(tableId);
//		hf = c.getDbFile(tableId);
//		hp = hf.readPage(0);
//	}
//	
//	@Test
//	public void testGetNumSlots() {
//	    final int PAGE_SIZE = HeapFile.PAGE_SIZE;
//
////	    formula (int)8 pagesize/(8 touplesize + 1) where tuplesize depends on data type and is fixed
//	    int correctNumSlots = (int) Math.floor(PAGE_SIZE * 8.0 / (td.getSize() * 8 +1));
//		assertEquals("check correct number of slots ", correctNumSlots, hp.getNumSlots());
//		
//	}



//	@Test
//	public void testGetHeaderSize() {
//		this.testGetNumSlots();
//		int numSlots = hp.getNumSlots();
//		double correctHeaderSizeBits = numSlots * 1.0 /8;
//		int correctHeaderSize = (int) Math.ceil(correctHeaderSizeBits);
//		assertEquals("check correct number of slots ", correctHeaderSize, hp.getHeaderSize());
//	}


	@Test
	public void testSlotOccupied() {

		try {
			for (int i=0; i<30; i++) {
				Tuple t = new Tuple(td);
				//(field number /data) => hashMap.put 
				t.setField(0, new IntField(new byte[] {0, 0, 0, (byte)131}));
				//input s  
				byte[] s = new byte[129];
				s[0] = 2;
				s[1] = 98;
				s[2] = 121;
				t.setField(1, new StringField(s));
				assertTrue(!hp.slotOccupied(t.getId()));
				hp.addTuple(t);	
				assertTrue(hp.slotOccupied(t.getId()));
			}
		
		} catch (Exception e) {
			fail("error when adding valid tuple");
			e.printStackTrace();
		}
		
		
		
		
	}
	
	

}
