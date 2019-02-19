package test;

//import static com.sun.tools.internal.ws.wsdl.parser.Util.fail;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;


import java.util.NoSuchElementException;

import hw1.Catalog;
import hw1.TupleDesc;
import hw1.Type;

public class CatalogTest {

	
	@org.junit.jupiter.api.Test
	public void test() {
		Catalog c = new Catalog();
		System.out.println(System.getProperty("user.dir"));
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		
		TupleDesc td = new TupleDesc(new Type[] { Type.INT, Type.STRING}, new String[] {"c1", "c2"});
		assertTrue(c.getTupleDesc(tableId).equals(td));
		assertTrue(c.getTableName(tableId).equals("test"));
		try {
			c.getDbFile(tableId);
		} catch(NoSuchElementException e) {
			fail("HeapFile not found");
		}
	}

}
