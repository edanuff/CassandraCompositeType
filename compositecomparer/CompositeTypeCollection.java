package compositecomparer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.MarshalException;

public class CompositeTypeCollection implements Collection<Object> {

	static final Logger logger = Logger.getLogger(CompositeTypeCollection.class
			.getName());

	byte[] bytes;
	ByteArrayOutputStream byteStream;
	DataOutputStream out;

	public CompositeTypeCollection() {
	}

	public CompositeTypeCollection(byte[] bytes) {
		this.bytes = bytes;
	}

	boolean isValidType(Object o) {
		if ((o instanceof UUID) || (o instanceof String) || (o instanceof Long)
				|| (o instanceof byte[]) || (o instanceof Boolean))
			return true;
		return false;
	}

	void initOutputStream() {
		if (byteStream == null) {
			byteStream = new ByteArrayOutputStream();
			out = new DataOutputStream(byteStream);
			if (bytes == null) {
				try {
					out.write(CompositeTypeUtils.COMPOSITETYPE_ID);
					out.write(CompositeTypeUtils.COMPOSITETYPE_VERSION);
				} catch (IOException e) {
					logger.throwing("CompositeTypeCollection",
							"initOutputStream", e);
				}
			} else {
				try {
					out.write(bytes);
				} catch (IOException e) {
					logger.throwing("CompositeTypeCollection",
							"initOutputStream", e);
				}
			}
		}
	}
	
	void pack() {
		if (byteStream != null) {
			bytes = byteStream.toByteArray();
			byteStream = null;
			out = null;
		}
	}

	public boolean add(Object o) {
		initOutputStream();
		if (o == null) {
			throw new NullPointerException();
		}
		if (!isValidType(o)) {
			throw new ClassCastException();
		}
		try {
			if (o instanceof Long)
				addLong(((Long) o).longValue());
			else if (o instanceof Boolean) {
				addBool(((Boolean) o).booleanValue());
			} else if (o instanceof String) {
				addUTF8((String) o);
			} else if (o instanceof UUID) {
				if (CompositeTypeUtils.isTimeBased((UUID) o)) {
					addTimeUUID((UUID) o);
				} else {
					addLexicalUUID((UUID) o);
				}
			} else if (o instanceof byte[]) {
				addBytes((byte[]) o);
			}
			return false;
		} catch (Exception e) {
			logger.throwing("CompositeTypeCollection", "add", e);
		}
		throw new IllegalArgumentException();
	}

	public boolean addAll(Collection<? extends Object> c) {
		for(Iterator<? extends Object> iter = c.iterator(); iter.hasNext();){
			add(iter.next());
		}
		return true;
	}

	/**
	 * Adds the provided byte array to the composite type being built.
	 * 
	 * @param part
	 *            the component part to append as a byte array
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addBytes(byte[] part) throws IOException {
		initOutputStream();
		out.write(CompositeTypeUtils.COMPONENT_BYTES);
		out.writeShort(part.length);
		out.write(part);
		return this;
	}

	/**
	 * Adds the provided ASCII string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as an ASCII string
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addAscii(String str) throws IOException {
		initOutputStream();
		byte[] bytes = bytes(str, CompositeTypeUtils.ASCII_ENCODING);
		out.write(CompositeTypeUtils.COMPONENT_ASCII);
		out.writeShort(bytes.length);
		out.write(bytes);
		return this;
	}

	/**
	 * Adds the provided UTF8 string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as a UTF8 string
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addUTF8(String str) throws IOException {
		initOutputStream();
		byte[] bytes = bytes(str);
		out.write(CompositeTypeUtils.COMPONENT_UTF8);
		out.writeShort(bytes.length);
		out.write(bytes);
		return this;
	}

	/**
	 * Adds the provided boolean value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a boolean value
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addBool(boolean val) throws IOException {
		initOutputStream();
		out.write(CompositeTypeUtils.COMPONENT_BOOL);
		out.writeBoolean(val);
		return this;
	}

	/**
	 * Adds the provided long value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a long value
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addLong(long val) throws IOException {
		initOutputStream();
		out.write(CompositeTypeUtils.COMPONENT_LONG);
		out.writeLong(val);
		return this;
	}

	/**
	 * Adds the time-based UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a time-based UUID
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addTimeUUID(UUID uuid) throws IOException {
		initOutputStream();
		byte[] bytes = bytes(uuid);
		out.write(CompositeTypeUtils.COMPONENT_TIMEUUID);
		out.write(bytes);
		return this;
	}

	/**
	 * Adds the lexical UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a regular UUID
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeCollection addLexicalUUID(UUID uuid) throws IOException {
		initOutputStream();
		byte[] bytes = bytes(uuid);
		out.write(CompositeTypeUtils.COMPONENT_LEXICALUUID);
		out.write(bytes);
		return this;
	}

	public CompositeTypeCollection addMatchMinimum() throws IOException {
		initOutputStream();
		out.write(CompositeTypeUtils.COMPONENT_MINIMUM);
		return this;
	}

	public CompositeTypeCollection addMatchMaximum() throws IOException {
		initOutputStream();
		out.write(CompositeTypeUtils.COMPONENT_MAXIMUM);
		return this;
	}

	public void clear() {
		this.bytes = null;
		this.byteStream = null;
		this.out = null;
	}

	public boolean contains(Object o) {
		for(Iterator<? extends Object> iter = iterator(); iter.hasNext();){
			Object obj = iter.next();
			if (obj.equals(o)) return true;
		}
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		for(Iterator<? extends Object> iter = c.iterator(); iter.hasNext();){
			Object obj = iter.next();
			if (!contains(obj)) return false;
		}
		return true;
	}

	public boolean isEmpty() {
		pack();
		if ((bytes != null) && (bytes.length > 2)) return false;
		return true;
	}

	public Iterator<Object> iterator() {
		pack();
		return new CompositeTypeIterator(this, bytes);
	}
	
	class CompositeTypeIterator implements Iterator<Object> {

		CompositeTypeCollection collection;
		byte[] bytes;
		int offset = 0;
		int len = 0;
		int type = 0;
		
		CompositeTypeIterator(CompositeTypeCollection c, byte[] bytes) {
			this.collection = c;
			this.bytes = bytes;
		}
		
		public boolean hasNext() {
			if (offset == 0) {
				if ((bytes == null) || (bytes.length == 0))
					return false;

				if ((bytes[0] & 0xFF) != CompositeTypeUtils.COMPOSITETYPE_ID) {
					throw new MarshalException("Not a composite type");
				}

				if ((bytes[1] & 0xFF) != CompositeTypeUtils.COMPOSITETYPE_VERSION) {
					throw new MarshalException(
							"Incorrect composite type version for this deserializer, expected "
									+ CompositeTypeUtils.COMPOSITETYPE_VERSION + ", found "
									+ (bytes[1] & 0xFF));
				}
				offset = 2;
			}
			return offset < bytes.length;
		}

		public Object next() {
			if (!hasNext()) throw new NoSuchElementException();
			int[] offsetRef = new int[1];
			offsetRef[0] = offset;
			Object obj = CompositeTypeUtils.deserializeBytesAt(bytes, offsetRef);
			offset = offsetRef[0];
			return obj;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}

	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		pack();
		List<Object> objects = CompositeTypeUtils.deserialize(bytes);
		return objects.size();
	}

	public Object[] toArray() {
		pack();
		List<Object> objects = CompositeTypeUtils.deserialize(bytes);
		return objects.toArray();
	}

	public <T> T[] toArray(T[] a) {
		pack();
		List<Object> objects = CompositeTypeUtils.deserialize(bytes);
		return objects.toArray(a);
	}
	
	public byte[] serialize() {
		pack();
		return bytes;
	}

	static byte[] bytes(UUID uuid) {
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}

	static byte[] bytes(String s) {
		return bytes(s, CompositeTypeUtils.UTF8_ENCODING);
	}

	static byte[] bytes(String s, String encoding) {
		try {
			return s.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException ", e);
			throw new RuntimeException(e);
		}
	}

}
