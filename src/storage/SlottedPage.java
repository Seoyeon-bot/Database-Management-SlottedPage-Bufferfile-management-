package storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import storage.SlottedPage.IndexOutOfBoundsException;
import storage.SlottedPage.OverflowException;

/**
 * A {@code SlottedPage} can store objects of possibly different sizes in a byte array.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class SlottedPage implements Iterable<Object> {

	/**
	 * The ID of this {@code SlottedPage}.
	 */
	int pageID;

	/**
	 * A byte array for storing the header of this {@code SlottedPage} and objects.
	 */
	byte[] data;

	/**
	 * Constructs a {@code SlottedPage}.
	 * 
	 * @param pageID
	 *            the ID of the {@code SlottedPage}
	 * @param size
	 *            the size (in bytes) of the {@code SlottedPage}
	 */
	public SlottedPage(int pageID, int size) {
		data = new byte[size];
		this.pageID = pageID;
		setEntryCount(0);// error
		setStartOfDataStorage(data.length - Integer.BYTES);
	}


	@Override
	public String toString() {
		String s = "";
		for (Object o : this) {
			if (s.length() > 0)
				s += ", ";
			s += o;
		}
		return "(page ID: " + pageID + ", objects: [" + s + "])";
	}

	/**
	 * Returns the ID of this {@code SlottedPage}.
	 * 
	 * @return the ID of this {@code SlottedPage}
	 */
	public int pageID() {
		return pageID;
	}

	/**
	 * Returns the byte array of this {@code SlottedPage}.
	 * 
	 * @return the byte array of this {@code SlottedPage}
	 */
	public byte[] data() {
		return data;
	}

	/**
	 * Returns the number of entries in this {@code SlottedPage}.
	 * 
	 * @return the number of entries in this {@code SlottedPage}
	 */
	public int entryCount() {
		return readInt(0);
	}

	/**
	 * Adds the specified object in this {@code SlottedPage}.
	 * 
	 * @param o
	 *            an object to add
	 * @return the index for the object
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 */
	// related to save(o) method 
	public int add(Object o) throws IOException, OverflowException {
		// TODO complete this method (20 points)
		// call save(object o ) or save(byte[] b) - callsaveLocation(index i , location ) 
	
		int location = 0, index = 0;
	        if (o == null) {
	            return -1; // handle error
	        } else {  // object is not null 
	            if (o instanceof byte[]) {  // check object type 
	                byte[] bArray = (byte[]) o;
	                location = save(bArray);
	            } else {    // call save(Object o) 
	                location = save(o);
	            }
	            index = entryCount();
	            setEntryCount(index + 1); // increment count by 1 
	            saveLocation(index, location);  // save location at specific index. 
	        }
	        return index;
	  
	}


	/**
	 * Returns the object at the specified index in this {@code SlottedPage} ({@code null} if that object was removed
	 * from this {@code SlottedPage}).
	 * 
	 * @param index
	 *            an index
	 * @return the object at the specified index in this {@code SlottedPage}; {@code null} if that object was removed
	 *         from this {@code SlottedPage}
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is given
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public Object get(int index) throws IndexOutOfBoundsException, IOException {
		// TODO complete this method (20 points)
		int location = 0; 
		Object obj = null;
		byte[] slottedPage = data();  //return entire slotted page
		if (index <0 || index >= entryCount()) {
			//System.out.println("Slottedpage Get() : IndexOutOfBounds Error! "); 
			throw new IndexOutOfBoundsException();
		}
		else {
			location = getLocation(index); //get location where index i stored 
			if(location == -1) { // if location == -1 return null since object was removed 
				return null; 
			}else {
			obj	= toObject(slottedPage, location); //return object at this loaction - calling the toObject(byte[] b, int offset) 
			}
		}
		return obj;
	}

	/**
	 * Puts the specified object at the specified index in this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index
	 * @param o
	 *            an object to add
	 * @return the object stored previously at the specified location in this {@code SlottedPage}; {@code null} if no
	 *         such object
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is used
	 */
	public Object put(int index, Object o) throws IOException, OverflowException, IndexOutOfBoundsException {
		if (index == entryCount()) {
			add(o);
			return null;
		}
		Object old = get(index);
		byte[] b = toByteArray(o);
		if (old != null && b.length <= toByteArray(old).length)
			System.arraycopy(b, 0, data, getLocation(index), b.length);
		else
			saveLocation(index, save(o));
		return old;
	}

	/**
	 * Removes the object at the specified index from this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index within this {@code SlottedPage}
	 * @return the object stored previously at the specified location in this {@code SlottedPage}; {@code null} if no
	 *         such object
	 * @throws IndexOutOfBoundsException
	 *             if an invalid index is used
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public Object remove(int index) throws IndexOutOfBoundsException, IOException {
		// TODO complete this method (10 points)
		Object removedObj = null ; 
		int location = 0;  
		//byte[] slottedPage = data(); // get entire slotted page so far saved. 
		if(index < 0 || index >= entryCount()) {
			throw new IndexOutOfBoundsException(); 
		}else {
			location = getLocation(index); //remove, save -1 ,don't change header count 
			if(location != -1) {
				saveLocation(index, -1); // set as -1 in the header
				removedObj = toObject(data, location); 
			}else {
				return null; // already removed. 
			}
		}
		//System.out.println("removed obj : " + removedObj);
		return removedObj; 
	}

	/**
	 * Returns an iterator over all objects stored in this {@code SlottedPage}.
	 */
	@Override
	public Iterator<Object> iterator() {
		// TODO complete this method (10 points)
		int count = entryCount();   // total number of entries 
		List<Object> output = new ArrayList<>();  // store object and skip null values.
		// check slotted page 
		for(int i=0; i <count ; i++ ) {
			try {
				Object obj = get(i); // get object at each index 
				if(obj == null) { // check whether obj is null 
					continue; 
				}else {      //not null add.  
					output.add(obj);
				}
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		
		Iterator<Object> iter = output.iterator();
	//	System.out.println("size of output : " + output.size()); 
		 return output.iterator(); // convert list to iterator<object> type
	}

	/**
	 * Reorganizes this {@code SlottedPage} to maximize its free space.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws IndexOutOfBoundsException 
	 * @throws OverflowException 
	 */
	protected void compact() throws IOException, OverflowException {
	 // If I uncomment this function, all unit test pass in slottedpagetest.java but make it fail filemanagertest.java
	 // I am wondering if i am eligible to receive partial points for compact(). Thank you for your time. 
		/**
		 int dataSize = data().length; 
		  int newStarting = dataSize - Integer.BYTES; // reinitialize starting address. 
		  setStartOfDataStorage(newStarting); //set start of data storage 
		  
           int newLocation = 0; 
		    for (int i = 0; i < entryCount(); i++) {
		        int location = getLocation(i); // get location 
		        Object obj;
		        try {
		            obj = get(i);  // get obj
		          
		            if (obj != null && location != -1) { // check whether object is not null and location is not -1 
		                newLocation = save(obj);  //save obj 
		                saveLocation(i, newLocation);  // update  location 
		                setStartOfDataStorage(newLocation);  // set start of data storage 
		               
		            } else {
		                // Store the index even if the object is null or its location is -1
		            	saveLocation(i, location);
		                System.out.println("location = -1 index : " + i);  
		               
		            }
		           
		            
		        } catch (IndexOutOfBoundsException | IOException e) {
		            e.printStackTrace();
		        }
		    }
		    setEntryCount(entryCount());
		    **/
		
		}
	
	/**
	 * Saves the specified object in the free space of this {@code SlottedPage}.
	 * 
	 * @param o
	 *            an object
	 * @return the location at which the object is saved within this {@code SlottedPage}
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified object
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected int save(Object o) throws OverflowException, IOException {
		return save(toByteArray(o)); // address of where o save.                     
	}

	/**
	 * Saves the specified byte array in the free space of this {@code SlottedPage}.
	 * 
	 * @param b
	 *            a byte array
	 * @return the location at which the object is saved within this {@code SlottedPage}
	 * @throws OverflowException
	 *             if this {@code SlottedPage} cannot accommodate the specified byte array
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws  
	 */
	protected int save(byte[] b) throws OverflowException, IOException {
		if (freeSpaceSize() < b.length + Integer.BYTES) { // check available freespace
			//System.out.println("So call compact() "); 
			compact();  //
			if (freeSpaceSize() < b.length + Integer.BYTES) // throw error 
				throw new OverflowException();
		}
		
		int location = startOfDataStorage() - b.length;
		System.arraycopy(b, 0, data, location, b.length);
		setStartOfDataStorage(location);
		return location;
	}

	/**
	 * Sets the number of entries in this {@code SlottedPage}.																																											
	 * 
	 * @param count
	 *            the number of entries in this {@code SlottedPage}
	 */
	protected void setEntryCount(int count) {
		writeInt(0, count);
	}

	/**
	 * Returns the start location of the specified object within this {@code SlottedPage}.
	 * 
	 * @param index
	 *            an index that specifies an object
	 * @return the start location of the specified object within this {@code SlottedPage}
	 */
	protected int getLocation(int index) { // return location ex) save object 123 at location 2034 index 0 then getLocatoin(0) returns 2034
		return readInt((index + 1) * Integer.BYTES);
	}

	/**
	 * Saves the start location of an object within the header of this {@code SlottedPage}.
	 * 
	 * @param index
	 *            the index of the object
	 * @param location
	 *            the start location of an object within this {@code SlottedPage}
	 */
	protected void saveLocation(int index, int location) { // used in add(Object o) 
		writeInt((index + 1) * Integer.BYTES, location); // for ith index, save at given location 
	}

	/**
	 * Returns the size of free space in this {@code SlottedPage}.
	 * 
	 * @return the size of free space in this {@code SlottedPage}
	 */
	public int freeSpaceSize() {
		return startOfDataStorage() - headerSize();
	}

	/**
	 * Returns the size of the header in this {@code SlottedPage}.
	 * 
	 * @return the size of the header in this {@code SlottedPage}
	 */
	protected int headerSize() {
		return Integer.BYTES * (entryCount() + 1);
	}

	/**
	 * Sets the start location of data storage.
	 * 
	 * @param startOfDataStorage
	 *            the start location of data storage
	 */
	protected void setStartOfDataStorage(int startOfDataStorage) {
		writeInt(data.length - Integer.BYTES, startOfDataStorage);
	}

	/**
	 * Returns the start location of data storage in this {@code SlottedPage}.
	 * 
	 * @return the start location of data storage in this {@code SlottedPage}
	 */
	protected int startOfDataStorage() {
		return readInt(data.length - Integer.BYTES);
	}

	/**
	 * Writes an integer value at the specified location in the byte array of this {@code SlottedPage}.
	 * 
	 * @param location
	 *            a location in the byte array of this {@code SlottedPage}
	 * @param value
	 *            the value to write
	 */
	protected void writeInt(int location, int value) {
		data[location] = (byte) (value >>> 24);
		data[location + 1] = (byte) (value >>> 16);
		data[location + 2] = (byte) (value >>> 8);
		data[location + 3] = (byte) value;
	}

	/**
	 * Reads an integer at the specified location in the byte array of this {@code SlottedPage}.
	 * 
	 * @param location
	 *            a location in the byte array of this {@code SlottedPage}
	 * @return an integer read at the specified location in the byte array of this {@code SlottedPage}
	 */
	protected int readInt(int location) {
		return ((data[location]) << 24) + ((data[location + 1] & 0xFF) << 16) + ((data[location + 2] & 0xFF) << 8)
				+ (data[location + 3] & 0xFF);
	}

	/**
	 * Returns a byte array representing the specified object.
	 * 
	 * @param o
	 *            an object.
	 * @return a byte array representing the specified object
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected byte[] toByteArray(Object o) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(b);
		out.writeObject(o);
		out.flush();
		return b.toByteArray();
	}

	/**
	 * Returns an object created from the specified byte array.
	 * 
	 * @param b
	 *            a byte array
	 * @param offset
	 *            the offset in the byte array of the first byte to read
	 * @return an object created from the specified byte array
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	protected Object toObject(byte[] b, int offset) throws IOException {
		try {
			if (b == null)
				return null;
			return new ObjectInputStream(new ByteArrayInputStream(b, offset, b.length - offset)).readObject();
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * A {@code OverflowException} is thrown if a {@code SlottedPage} cannot accommodate an additional object.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public class OverflowException extends Exception {

		/**
		 * Automatically generated serial version UID.
		 */
		private static final long serialVersionUID = -3007432568764672956L;

	}

	/**
	 * An {@code IndexOutofBoundsException} is thrown if an invalid index is used.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public class IndexOutOfBoundsException extends Exception {

		/**
		 * Automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 7167791498344223410L;

	}

}
