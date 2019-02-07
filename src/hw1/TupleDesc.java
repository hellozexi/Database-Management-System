package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
        //edgeKs
        this.types = typeAr;
        this.fields = fieldAr;

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        try{
            return fields[i];

        } catch(NoSuchElementException e){
            System.out.println("no such element found");
            throw e;
        }

    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	
        try{
        	if (name.length()>0) {
	            int index = -1;
	            for (int i = 0; i< this.fields.length;i++){
	                if (this.fields[i].equals(name)){
	                    index = i;
	                }
	            }
	            return index;
        	}
        	else {
        		throw new NoSuchElementException("input length is not valid");
        	}

        } catch(NoSuchElementException e){
            throw e;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        try{
//        	System.out.println(this.types[i]);
            return this.types[i];
        } catch(NoSuchElementException e){
            throw e;
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */



//	int 4 bytes
//	str itself 128 bytes
//  a tuple eg
//	5hello (size 1byte + str 128(may include padding if needed))
//	1 + 128

    public int getSize() {
        try{
            final int INT_SIZE_BYTE = 4;
            final int STR_SIZE_BYTE = 129;
            int size =0;

            for (int i=0; i< this.fields.length;i++){
                if (this.types[i].equals(Type.INT)) {
                    size = size + INT_SIZE_BYTE;
                }
                else if (this.types[i].equals(Type.STRING)){
                    size = size + STR_SIZE_BYTE;
                }
                else{
                    continue;
                }
            }
            return size;

        } catch(Exception e){
            return 0;
        }
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if 
     * they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    
    	//check if type arr have same length
    	if (this.types.length != ((TupleDesc) o).types.length) {return false;}
    	
        //check if same instance
        if( o instanceof TupleDesc) {
            //check if same size
            if(((TupleDesc) o).getSize() == this.getSize()){
                
            	//check same nth type
                for (int i =0; i <this.types.length; i++){
                	//System.out.println(this.types[i]);
                    if (!this.types[i].equals(((TupleDesc) o).types[i])) { return false; }
                }
            }
            return true;
        }
        else{
            return false;
        }

    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        String toStr = "";

        for (int i=0; i<this.fields.length;i++){
            if (this.fields[i] != null){
                toStr += this.types[i] + "(" + this.fields[i]+")";
            }
            if (i<this.fields.length-1) {  toStr += ",";  }

        }

        return toStr;
    }
}
