
package storage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import storage.SlottedPage.IndexOutOfBoundsException;
import storage.SlottedPage.OverflowException;
import storage.StorageManager.InvalidLocationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import storage.SlottedPage;
/**
 * A {@code BufferedFileManager} manages a storage space using the slotted page format and buffering.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class BufferedFileManager extends FileManager {
	// TODO complete this class (5 points)
	
	// I picked least recently used (LRU) page eviction policy, 
	//so whenever I add slottedapge to buffer or access specific slottedpage from buffer, I updated most recently 
	// use slottedpages address inLRUStack, so when buffer is full, I call ePolicy() which is evaction policy to make space in buffer. 
	// Also I always check whther buffer has space, before i add elemented into buffer and upated stack to track which locatio nand slottedpage we used. 
	
	
	private final int bufferSize;
    private static final Stack<Long> LRUStack = new Stack<>();; // long consit with pageID + index 
    FileManager fm = new FileManager();  // call filemanaer 
    private static final Map<Long, SlottedPage> buffer = new HashMap<>(); // location, slottedpage 
   
    
	/**
	 * Constructs a {@code BufferedFileManager}.
	 * 
	 * @param slottedPageSize
	 *            the size (in bytes) of {@code SlottedPage}s
	 * @param bufferSize
	 *            the number of {@code SlottedPage}s that the buffer can maintain
	 */
	public BufferedFileManager(int slottedPageSize, int bufferSize) {
		super(slottedPageSize);
		 this.bufferSize = bufferSize; 
	  
	    }
	
	
	
    
	// readPage if buffer has that page, if not call filemanager to get slottedpage and save on buffe update LRU, return that request page 
	/**
	 * Find needPage. if needed slottedpage exist in buffer return else find from disk, add to  buffer and return needed slotted page.
	 * @param fileID
	 * @param pageID
	 * @param index 
	 * @return specific object with specific fileID, pageID and index locatino in slotted page. 
	 * @throws IOException
	 */
	public SlottedPage readPage(int fileID, Long location) throws IOException {
		
		SlottedPageFile file = null;
		SlottedPage needpage = null; 
		
		needpage = buffer.get(location); 
		if(needpage != null) {
			// buffer has this slottedpage. 
			updateLRU(location); 
			return needpage; 
		}else {
			// buffer don't have this slotted page >> find in disk. 
			needpage = fm.page(fileID, first(location)); 
			if(needpage != null) {
				// add to buffer 
				buffer.put(location, needpage);
				updateLRU(location); 
			}
			return needpage;
		}
	}
		
    public int first(long l) {
		return (int) (l >> 32);
    }
    public int second(long l) {
		return (int) l;
    }
	
	
	/**
	 * search specific slottedPage with given parameters on the disk. 
	 * @param fileID
	 * @param location
	 * @return specific object
	 */
	private SlottedPage SearchPageInDisk(int fileID, Long location ) {
		// disk search  
		SlottedPageFile file = null; 
		SlottedPage page = null; 
		try {
			file = fm.file(fileID);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		if(file !=null) {
			try {
				page = fm.page(fileID, first(location));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			if(page!= null) {
			     return page; 
			}
		}
		
		return null ; 
	}
	
	
	//writeToBuffer  same as add() 
	/**
	 * Write down specific slotted page in buffer hashmap <long, slottedPage > 
	 * @param fileID
	 * @param location
	 * @throws IOException
	 */
	private void writeToBuffer( int fileID , Long location, Object object ) throws IOException {
		// find slotted page with given location and write on to buffer, if buffer is full use eviction policy
		SlottedPageFile file = fm.file(fileID);
		int pageID = fm.first(location); 
		SlottedPage newPage =  fm.page(fileID, pageID); // get slotted page 
	   
		try {
			newPage.add(object);
		} catch (IOException | OverflowException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		// check buffer size. 
		int currentSize = buffer.size(); 
		if(currentSize >= bufferSize) {
			// buffer is full, call ePolicy - uses LRU ( removed least recently used slottedpage from the buffer hash map ) 
		    ePolicy(); 
		}
	  
		buffer.put(location, newPage);
		updateLRU(location); 
	
		System.out.println("[writeToBuffer] : location : " + location + " object : " + object); 
	}
	
	/**
	 * remove least recently used location and slotted page from the buffer to create space in buffer. 
	 */
	private void ePolicy() {
		 if (!LRUStack.isEmpty()) {
		        Long removeElement = LRUStack.pop();  // least recently used element.
		        buffer.remove(removeElement);
		      //  System.out.println("[ePolicy()] : Removed " + removeElement + " from the buffer");
		    } else {
		      //  System.out.println("[ePolicy()] : LRUStack is empty, no elements to remove from the buffer");
		    }
	}
	
	/**
	 * updaet LRU stack. 
	 * @param location
	 */
	private void updateLRU(Long location) {
		 LRUStack.remove(location); // if location is in stack, remove first and push to the top. 
		 LRUStack.push(location);
		// System.out.println("[updateLRU] : push location pageID : " + first(location) + " index: " + second(location) + " on the top of stack. ");	
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
	public Long add(int fileID, Long location , Object o) throws IOException {
		SlottedPage page = null; 
		SlottedPageFile file = null; 
		file = fm.file(fileID);
		
		if(buffer.containsKey(location)) {  // buffer has this slotted page 
			page = buffer.get(location); 
			System.out.println("buffer.getlocatoin page : " + page);
			if (page != null) {
				try {
					location = concatenate(page.pageID(), page.add(o)); // pageID() null error
				} catch (IOException | OverflowException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else {
				// page is null, add new page to buffer 
				// no page yet. 
				page = new SlottedPage(0, slottedPageSize); 
				System.out.println("page is null "); 
			}
	    }
	    int currentSize = buffer.size(); // check buffer size 
		if(currentSize >= bufferSize) { // full call eviction policy 
			//System.out.println("add call eviction policy() to make space in buffer. bufferSize: " + bufferSize + " currentSize : " + currentSize); 
			 ePolicy(); 
		}
		   buffer.put(location, page); 
		 //  System.out.println("[add] add page to buffer. pid : "+ first(location) + " index : " + second(location)); 
		   updateLRU(location);
		   return location;
	}
	
	@Override
	public Object put(int fileID, Long location, Object o) throws IOException, InvalidLocationException {
	SlottedPage page = null; 
	Object obj = null; 
	if(first(location)<0 || second(location) < 0) {
		throw new InvalidLocationException();
	}
	page = buffer.get(location); 
	if(page == null) {
		// page doesn't exist yet -> create new page 
		page = new SlottedPage(first(location), slottedPageSize); 
	}
	try {
		obj = page.put(second(location), obj);
	} catch (IOException | OverflowException | IndexOutOfBoundsException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
	// check buffer size 
    int currentSize = buffer.size(); 
	if(currentSize >= bufferSize) { // full call eviction policy 
		System.out.println("put called epolicy()"); 
		 ePolicy(); 
	}
	buffer.put(location, page);
	// System.out.println("[put] add page to buffer. pid : "+ first(location) + " index : " + second(location));  
	updateLRU( location); 
//	System.out.println("[put] put page in buffer"); 
	fm.updated(page, fileID);
	return obj; 
	
	}
	
	// remove 
	 @Override
	 public Object get(int fileID, Long location) throws IOException, InvalidLocationException {
     // get in buffer if it has or get it from disk and add to buffer and update stack. 
		 SlottedPage page = null; 
		 Object obj = null; 
		 if(buffer.containsKey(location)) {
			 try {
				obj = page.get(second(location));
			} catch (IndexOutOfBoundsException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			 return obj; 
		 }else {
			 // get from disk 
			 page = fm.page(fileID, first(location)); 
			 if(page !=null) {
					 try {
						obj = page.get(second(location));
					} catch (IndexOutOfBoundsException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					 // check buffer size 
					 int currentSize = buffer.size(); 
						if(currentSize >= bufferSize) { // full call eviction policy 
							System.out.println("put called epolicy()"); 
							 ePolicy(); 
						}
					 // add to buffer 
					 buffer.put(location, page); 
					 updateLRU(location);  
			 }
		 }
		 return obj;  
	 }
   
    /**
     * remove object from this buffer and BufferId2file
     * @ return removed object 
     */
	 @Override
    public Object remove(int fileID, Long location) throws IOException, InvalidLocationException {
    	// If location is in buffermap and buffer, Remove object from the buffer, update LRU info
     Object removedObj = null; 
     SlottedPage page = null; 
      if(buffer.containsKey(location)) { // remove from buffer 
    	  removedObj = buffer.get(location); 
    	  page =  buffer.remove(location); 
          updateLRU(location); 
          System.out.println("[remove] removed in buffer"); 
         removedObj = super.remove(fileID, location);
      }else {
    	  removedObj = super.remove(fileID, location);
    	  SlottedPageFile file  = super.file(fileID); 
    	  page = file.get(first(location)); 
    	  
	  	  int currentSize = buffer.size(); 
	  	  if(currentSize >= bufferSize) { // full call eviction policy 
	  			System.out.println("remove called epolicy()"); 
	  			 ePolicy(); 
	  		}
	  	  
    	  buffer.put(location, page);  // add to buffer. 
    	  updateLRU(location);
    	//  checkBuffer(); 
    	  
    	  System.out.println("[remove] don't have this object so find from disk, add to buffer obj : " + removedObj); 
          System.out.println("[remove] we add slottedpage to buffer"); 
          System.out.println("\n"); 
        }
       
        return removedObj;
      }
   
// helper method check buffer 
    private void checkBuffer() {
    	for (Map.Entry<Long, SlottedPage> entry : buffer.entrySet()) {
    		Long location = entry.getKey(); 
    		int pid = first(location); 
    		int index = second(location); 
            System.out.println("Buffer has pid: " + pid +  " index:  " + index +" slottedpage : " + entry.getKey());
    }
    }
    // Method to iterate over all objects stored in a file
    /**
     * iterate objects in buffer and return it as Iterator<object> type
     */
    public Iterator<Object> iterator() {
    	// if location is in buffer iterate over that slotted page 
    	// if Bufferid2fiel has fileId 
    	SlottedPage p = null; 
    	Iterator<Object> iter = null; 
    	SlottedPageFile pfile = null; 
    	// iterate over buffer. 
    	List<Object> list = new ArrayList<Object>(); 
    	for (Map.Entry<Long, SlottedPage> entry : buffer.entrySet()) {
    		p = entry.getValue(); 
    		Object obj = null; 
    		if(p != null) {
    			try {
					obj = p.get(second(entry.getKey()));
					list.add(obj); 
				} catch (IndexOutOfBoundsException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
    		}
    	}
    	return list.iterator(); 
    	
     }
    // Method to shut down the FileManager
    @Override
    /**
     * shutdown the  buffer file manager
     */
    public void shutdown() throws IOException {
    	super.shutdown();
    }
    
  
}
