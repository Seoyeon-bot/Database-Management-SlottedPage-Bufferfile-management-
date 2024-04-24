package storage.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import storage.SlottedPage;
import storage.SlottedPage.IndexOutOfBoundsException;
import storage.SlottedPage.OverflowException;

/**
 * This program tests the {@code SlottedPage} class.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 * 
 */
public class SlottedPageTest {

	/**
	 * The size (in bytes) of {@code SlottedPage}s.
	 */
	public static int slottedPageSize = 2048;

	/**
	 * Tests {@link SlottedPage#add(Object)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void add() throws Exception {
		SlottedPage p = new SlottedPage(0, slottedPageSize);
		add(p, "123", 0);
		add(p, "456", 1);
		add(p, "789", 2);
	}

	/**
	 * Tests {@link SlottedPage#get(int)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void get() throws Exception {
		SlottedPage p = new SlottedPage(0, slottedPageSize);
		p.add("123");
		p.add("456");
		p.add("789");
		byte[] data = p.data();
		writeInt(data, (1 + 1) * Integer.BYTES, -1);
		assertEquals(3, readInt(data, 0));
		assertTrue(get(p, -1) instanceof IndexOutOfBoundsException);
		assertEquals("123", get(p, 0));
		assertEquals(null, get(p, 1));
		assertEquals("789", get(p, 2));
		assertTrue(get(p, 3) instanceof IndexOutOfBoundsException);
	}

	/**
	 * Tests {@link SlottedPage#remove(int)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void remove() throws Exception {
		SlottedPage p = new SlottedPage(0, slottedPageSize);
		p.add("123");
		p.add("456");
		p.add("789");
		assertEquals("456", remove(p, 1));
		assertEquals(null, remove(p, 1));
		assertEquals(3, readInt(p.data(), 0));
		assertTrue(get(p, -1) instanceof IndexOutOfBoundsException);
		assertEquals("123", get(p, 0));
		assertEquals(null, get(p, 1));
		assertEquals("789", get(p, 2));
		assertTrue(get(p, 3) instanceof IndexOutOfBoundsException);
	}

	/**
	 * Tests {@link SlottedPage#iterator()}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void iterator() throws Exception {
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("123");
		list.add("456");
		list.add("789");
		SlottedPage p = new SlottedPage(0, slottedPageSize);
		for (Object o : list)
			p.add(o);
		assertEquals(list, list(p.iterator()));
		list.remove(1);
		p.remove(1);
		assertEquals(list, list(p.iterator()));
	}

	/**
	 * Tests {@link SlottedPage#compact()}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void compact() throws Exception {
		SlottedPage p = new SlottedPage(0, slottedPageSize * 2);
		try {
			for (int i = 0; i < Integer.MAX_VALUE; i++)
				p.add(i);
		} catch (IOException | OverflowException e) {
			// e.printStackTrace();
		}
		ArrayList<Object> l = list(p.iterator());
		int index = 3;
		Object o = l.set(index, null);
		try {
			p.remove(index);
		} catch (IndexOutOfBoundsException | IOException e) {
			// e.printStackTrace();
		}
		try {
			l.add(o);
			p.add(o);
		} catch (IOException | OverflowException e) {
			e.printStackTrace();
		}
	
		assertEquals(l.size(), p.entryCount());
		for (int i = 0; i < l.size(); i++) {
			assertEquals(l.get(i), p.get(i));
		}
			
	}


	/**
	 * Adds a {@link String} to a {@link SlottedPage} and checks if the added {@link String} at the specified index
	 * 
	 * @param p
	 *            a {@link SlottedPage}
	 * @param s
	 *            a {@link String}
	 * @param i
	 *            an index
	 * @throws Exception
	 *             if an error occurs
	 */
	void add(SlottedPage p, String s, int i) throws Exception {
		int index = p.add(s);
		assertEquals(i, index);
		byte[] data = p.data();
		assertEquals(index + 1, readInt(data, 0));
		Object o = null;
		o = toObject(data, readInt(data, Integer.BYTES * (index + 1)));
		assertEquals(s, o);
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
	private Object toObject(byte[] b, int offset) throws IOException {
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
	 * Reads an integer at the specified location in the specified byte array.
	 * 
	 * @param data
	 *            a byte array
	 * @param location
	 *            a location in the byte array
	 * @return an integer read at the specified location in the specified byte array
	 */
	int readInt(byte[] data, int location) {
		return ((data[location]) << 24) + ((data[location + 1] & 0xFF) << 16) + ((data[location + 2] & 0xFF) << 8)
				+ (data[location + 3] & 0xFF);
	}

	/**
	 * Returns an object at the specified index in the specified {@link SlottedPage}.
	 * 
	 * @param p
	 *            a {@link SlottedPage}
	 * @param i
	 *            an index
	 * @return an object at the specified index in the specified {@link SlottedPage}
	 */
	Object get(SlottedPage p, int i) {
		try {
			return p.get(i);
		} catch (IndexOutOfBoundsException | IOException e) {
			return e;
		}
	}

	/**
	 * Writes an integer value at the specified location in the specified byte array.
	 * 
	 * @param data
	 *            a byte array
	 * @param location
	 *            a location in the byte array
	 * @param value
	 *            the value to write
	 */
	void writeInt(byte[] data, int location, int value) {
		data[location] = (byte) (value >>> 24);
		data[location + 1] = (byte) (value >>> 16);
		data[location + 2] = (byte) (value >>> 8);
		data[location + 3] = (byte) value;
	}

	/**
	 * Removes an object from the specified {@link SlottedPage}
	 * 
	 * @param p
	 *            a {@link SlottedPage}
	 * @param i
	 *            the index of the object to remove
	 * @return the object removed
	 */
	Object remove(SlottedPage p, int i) {
		try {
			return p.remove(i);
		} catch (IndexOutOfBoundsException | IOException e) {
			return e;
		}
	}

	/**
	 * Returns an {@link ArrayList} consisting of the elements accessible through the specified {@link Iterator}.
	 * 
	 * @param i
	 *            an {@link Iterator}
	 * @return an {@link ArrayList} consisting of the elements accessible through the specified {@link Iterator}
	 */
	public static ArrayList<Object> list(Iterator<Object> i) {
		ArrayList<Object> l = new ArrayList<Object>();
		while (i.hasNext())
			l.add(i.next());
		return l;
	}

}
