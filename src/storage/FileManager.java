package storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import storage.SlottedPage.IndexOutOfBoundsException;
import storage.SlottedPage.OverflowException;
import storage.StorageManager.InvalidLocationException;
import storage.test.SlottedPageTest;

import java.io.*;

/**
 * A {@code FileManager} manages a storage space using the slotted page format.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class FileManager implements StorageManager<Long, Object> {

	/**
	 * A map that associates the ID of each file with a {@code SlottedPageFile} for accessing that file.
	 */
	Map<Integer, SlottedPageFile> id2file = new HashMap<Integer, SlottedPageFile>();

	/**
	 * The size (in bytes) of {@code SlottedPage}s.
	 */
	int slottedPageSize;

	/**
	 * Constructs a {@code FileManager}.
	 * 
	 * @param slottedPageSize
	 *            the size (in bytes) of {@code SlottedPage}s
	 */
	public FileManager(int slottedPageSize) {
		this.slottedPageSize = slottedPageSize;
	}
	
	public FileManager() {
		// TODO Auto-generated constructor stub
	}
	/**
	 * Returns the first location in any file.
	 * 
	 * @return the first location in any file.
	 */
	@Override
	public Long first() {
		return 0L;
	}

	/**
	 * Shuts down this {@code FileManager} after saving all of the essential in-memory data on disk.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void shutdown() throws IOException {
		for (SlottedPageFile f : id2file.values())
			f.close(); // closes each data file
	}

	@Override
	public String toString() {
		return id2file.values().toString();
	}

	/**
	 * Adds the specified object at the end of the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 * @param o
	 *            the object to add
	 * @return the location of the object in the specified file
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	@Override
	public Long add(int fileID, Object o) throws IOException {
		int size = size(fileID); // the number of pages in the data file
		SlottedPage p;
		long location;
		try {
			if (size == 0) { // if no page yet
				p = new SlottedPage(0, slottedPageSize); // create page 0
				location = concatenate(p.pageID(), p.add(o)); // add the object in the page
			} else { // existing page
				p = page(fileID, size - 1); // get last page
				try {
					location = concatenate(p.pageID(), p.add(o)); // add the object in the page
				} catch (OverflowException e) {    // if the object cannot fit into the page
					p = new SlottedPage(p.pageID() + 1, slottedPageSize); // create a new page
					location = concatenate(p.pageID(), p.add(o)); // add the object in the new page
				}
			}
		
		} catch (OverflowException e) {
			throw new IOException(e);
		}
		updated(p, fileID); // inform that the page is updated (and thus the page will eventually be saved in the file)
		return location; // return the location of the object
	}

	/**
	 * Puts the specified object at the specified location in the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 * @param location
	 *            the location of the object
	 * @param o
	 *            the object to put
	 * @return the object stored previously at the specified location in the specified file; {@code null} if no such
	 *         object
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws InvalidLocationException
	 *             if an invalid location is given
	 */
	@Override
	public Object put(int fileID, Long location, Object o) throws IOException, InvalidLocationException {
		// SlottedPage p = page(fileID, first(location)); // the page specified by the 1st half of the location
		// TODO complete this method (10 points)
		int idOfPage = first(location);
	    int indexWithinPage = second(location);
	    Object previousObject = null;
	 
	    if (idOfPage < 0 || indexWithinPage < 0) {   // invalid location type throw error.
	        throw new InvalidLocationException();
	    }
	    try {
	    SlottedPageFile pfile = file(fileID); // Find the SlottedPageFile
	    SlottedPage page = page(fileID, idOfPage); // Find the right page

	    if (page == null) { // Page doesn't exist yet -> create new page.
	        page = new SlottedPage(idOfPage, slottedPageSize); // Create a new page with the specified ID
	        pfile.save(page); // Save the page into the SlottedPageFile
	    }
	        previousObject = page.put(indexWithinPage, o); // Put object 'o' at the specified index
	        updated(page, fileID); //update page with specific file id. 

	    } catch (IndexOutOfBoundsException e) {   // handle error. 
	        throw new InvalidLocationException();
	    } catch (OverflowException e) {
	        e.printStackTrace();
	    }
	     return previousObject;
	}
	

	/**
	 * Returns the object at the specified location in the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 * @param location
	 *            the location of the object
	 * @return the object at the specified location in the specified file
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws InvalidLocationException
	 *             if an if an invalid location is given
	 */
	@Override
	public Object get(int fileID, Long location) throws IOException, InvalidLocationException {
		// SlottedPage p = page(fileID, first(location)); // the page specified by the 1st half of the location
		// TODO complete this method (5 points)
		
		int pageID = first(location); // page id first 4 
	    int index = second(location); // second half = index 
     
	    if(pageID < 0 ) {
	    	throw new InvalidLocationException();
	    }
	    
	    SlottedPage page = page(fileID, pageID); 
	    //System.out.println("[get] : find page (fileid : " + fileID + " pageID : " + pageID+")"); 
        if (page == null) {      // case if page in null 
            throw new InvalidLocationException();
        }
        
        if (index < 0 || index >= page.entryCount()) {     // Check if index is valid
            throw new InvalidLocationException();
        }
        
        Object obj = null;
		try {
			obj = page.get(index);
			//System.out.println("get object at index : " + index + " obj : " + obj+ "\n"); 
			if (obj != null) {
				return obj;
	        }else {
	        	return null;
	        }
		} catch (IndexOutOfBoundsException e) {
	        return null; // Return null if index is out of bounds
	    } 
	}
	

	
	/**
	 * Removes the specified object at the specified location in the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 * @param location
	 *            the location of the object
	 * @return the object stored previously at the specified location in the specified file; {@code null} if no such
	 *         object
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws InvalidLocationException
	 *             if an if an invalid location is given
	 */
	@Override
	public Object remove(int fileID, Long location) throws IOException, InvalidLocationException {
		// SlottedPage p = page(fileID, first(location)); // the page specified by the 1st half of the location
		// TODO complete this method (5 points)
	
		int pageID = first(location); // page id first 4 
		int index = second(location); // second half = index 
			
			    if(pageID < 0 ) {
			    	throw new InvalidLocationException();
			    }
			    SlottedPage page = page(fileID, pageID); 
		        if (page == null) {      // case if page in null 
		            throw new InvalidLocationException();
		        }
		      
		        if (index < 0 || index >= page.entryCount()) {// Check if index is valid
		            throw new InvalidLocationException();
		        }
		        
		        // Remove the object at the specified index from the page
		        Object removedObject = null;
				try {
					removedObject = page.remove(index);
					Object checkObj = page.get(index); 
				} catch (IndexOutOfBoundsException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        updated(page, fileID);    // Save the updated page back to the file
		        return removedObject; // Return the removed object
		    
		}
	/**
	 * Removes all data from the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	@Override
	public void clear(int fileID) throws IOException {
		SlottedPageFile f = file(fileID);
		f.clear();
	}

	/**
	 * Returns an iterator over all objects stored in the the specified file.
	 * 
	 * @param fileID
	 *            the ID of the file
	 */
	@Override
	public Iterator<Object> iterator(int fileID) {
		// TODO complete this method (5 points)
		
		SlottedPageFile pfile = null;  SlottedPage p = null; 
	    Iterator<Object> iterators = null; 
	    ArrayList<Object> list = new ArrayList<Object>(); 
	    
	    try {
			pfile = file(fileID);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 

	    if(pfile != null) {
	        try {
	            for(int i=0; i <pfile.size(); i++) {
	                  p = page(fileID, i); 
	                 Iterator<Object> iter = p.iterator(); 
	                while(iter.hasNext()) {
	                    list.add(iter.next()); 
	                }
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    Iterator<Object> iterOfList = list.iterator();          
	    return iterOfList; 
	}

	/**
	 * Returns the number of {@code SlottedPage}s in the specified {@code SlottedPageFile}.
	 * 
	 * @param fileID
	 *            the ID of the {@code SlottedPageFile}
	 * @return the number of {@code SlottedPage}s in the specified {@code SlottedPageFile}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	int size(int fileID) throws IOException {
		return file(fileID).size();
	}

	/**
	 * Returns the specified {@code SlottedPage} ({@code null} if no such {@code SlottedPage}).
	 * 
	 * @param fileID
	 *            the ID of the file containing the {@code SlottedPage}
	 * @param pageI
	 *            the ID of the {@code SlottedPage}
	 * @return the specified {@code SlottedPage}; {@code null} if no such {@code SlottedPage}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	SlottedPage page(int fileID, int pageID) throws IOException {
		SlottedPageFile f = file(fileID);
		return f.get(pageID);
	}

	/**
	 * Is invoked when the specified {@code SlottedPage} is updated.
	 * 
	 * @param p
	 *            a {@code SlottedPage}
	 * @param fileID
	 *            the ID of the file containing the {@code SlottedPage}
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	void updated(SlottedPage p, int fileID) throws IOException {
		SlottedPageFile f = file(fileID);
		f.save(p);
	}

	/**
	 * Returns a {@code long} value obtaining by concatenating the given {@code int} values
	 * 
	 * @param i
	 *            an {@code int} value
	 * @param j
	 *            an {@code int} value
	 * @return a {@code long} value obtaining by concatenating the given {@code int} values
	 */
	protected long concatenate(int i, int j) {
		return (((long) i) << 32) | j;
	}

	/**
	 * Returns an {@code int} value obtained from the first 4 bytes of the given {@code long} value.
	 * 
	 * @param l
	 *            a {@code long} value.
	 * @return an {@code int} value obtained from the first 4 bytes of the given {@code long} value
	 */
	protected int first(long l) {
		return (int) (l >> 32);
	}

	/**
	 * Returns an {@code int} value obtained from the last 4 bytes of the given {@code long} value.
	 * 
	 * @param l
	 *            a {@code long} value.
	 * @return an {@code int} value obtained from the last 4 bytes of the given {@code long} value
	 */
	protected int second(long l) {
		return (int) l;
	}

	/**
	 * Returns the {@code SlottedPageFile} corresponding to the specified file ID.
	 * 
	 * @param fileID
	 *            the ID of a {@code SlottedPageFile}
	 * @return the {@code SlottedPageFile} corresponding to the specified file ID
	 * @throws FileNotFoundException
	 *             if the specified file cannot be found/created
	 * @throws IOException
	 *             if an IO error occurs
	 */
	protected SlottedPageFile file(int fileID) throws FileNotFoundException, IOException {
		SlottedPageFile file = id2file.get(fileID);
		if (file == null) {
			file = new SlottedPageFile(fileID + ".dat", slottedPageSize);
			id2file.put(fileID, file);
		}
		return file;
	}

}
