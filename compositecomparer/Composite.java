package compositecomparer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.MarshalException;

/**
 * Composite type for Cassandra columns.
 * <p>
 * Composite allows you to combine the existing Cassandra
 * types into a composite type that will then be compared correctly for each of
 * the component types.
 * <p>
 * To construct a composite name for a new column, use the following:
 * <p>
 * <code>
 * Composite c = new Composite();<br>
 * c.addUTF8("smith").addUTF8("bob").addLexicalUUID(new UUID());<br>
 * byte[] bytes = c.serialize();<br>
 * </code>
 * <p>
 * The actual composite type consists of 1 byte to specify the component type,
 * and then for variable length types such as ASCII strings, 2 bytes are used
 * for each string length.
 * 
 */

public class Composite implements Collection<Object>, Comparable<Composite> {

	public final static int COMPOSITETYPE_ID = 0xED;

	public final static int COMPOSITETYPE_VERSION = 1;

	public final static int COMPONENT_MINIMUM = 0;

	public final static int COMPONENT_BOOL = 1;

	public final static int COMPONENT_LONG = 2;

	public final static int COMPONENT_TIMEUUID = 3;

	public final static int COMPONENT_LEXICALUUID = 4;

	public final static int COMPONENT_ASCII = 5;

	public final static int COMPONENT_UTF8 = 6;

	public final static int COMPONENT_BYTES = 7;

	public final static int COMPONENT_MAXIMUM = 255;

	public static final String UTF8_ENCODING = "UTF-8";

	public static final String ASCII_ENCODING = "US-ASCII";

	static final Logger logger = Logger.getLogger(Composite.class.getName());

	byte[] bytes;
	ByteArrayOutputStream byteStream;
	DataOutputStream out;

	public Composite() {
	}

	public Composite(byte[] bytes) {
		this.bytes = bytes;
	}

	public Composite(Object... objects) {
		for (Object obj : objects) {
			if (obj instanceof Long) {
				addLong(((Long) obj).longValue());
			} else if (obj instanceof Integer) {
				addLong(((Integer) obj).intValue());
			} else if (obj instanceof Boolean) {
				addBool(((Boolean) obj).booleanValue());
			} else if (obj instanceof String) {
				addUTF8((String) obj);
			} else if (obj instanceof UUID) {
				if (isTimeBased((UUID) obj)) {
					addTimeUUID((UUID) obj);
				} else {
					addLexicalUUID((UUID) obj);
				}
			} else if (obj instanceof byte[]) {
				addBytes((byte[]) obj);
			}
		}
	}

	private void initOutputStream() {
		if (byteStream == null) {
			byteStream = new ByteArrayOutputStream();
			out = new DataOutputStream(byteStream);
			if (bytes == null) {
				try {
					out.write(COMPOSITETYPE_ID);
					out.write(COMPOSITETYPE_VERSION);
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

	private void pack() {
		if (byteStream != null) {
			bytes = byteStream.toByteArray();
			byteStream = null;
			out = null;
		}
	}

	public String toString() {
		Iterator<Object> iter = iterator();
		if (!iter.hasNext())
			return "";
		StringBuilder builder = new StringBuilder(stringValueOf(iter.next()));
		while (iter.hasNext())
			builder.append(',').append(stringValueOf(iter.next()));
		return builder.toString();
	}

	private static String stringValueOf(Object o) {
		if (o instanceof byte[])
			return getHexString((byte[]) o, 0, ((byte[]) o).length);
		return String.valueOf(o);
	}

	public boolean add(Object o) {
		if (o == null) {
			throw new NullPointerException();
		}
		if (o instanceof Long)
			addLong(((Long) o).longValue());
		else if (o instanceof Boolean) {
			addBool(((Boolean) o).booleanValue());
		} else if (o instanceof String) {
			addUTF8((String) o);
		} else if (o instanceof UUID) {
			if (isTimeBased((UUID) o)) {
				addTimeUUID((UUID) o);
			} else {
				addLexicalUUID((UUID) o);
			}
		} else if (o instanceof byte[]) {
			addBytes((byte[]) o);
		} else {
			throw new ClassCastException();
		}

		return true;
	}

	public boolean addAll(Collection<? extends Object> c) {
		for (Iterator<? extends Object> iter = c.iterator(); iter.hasNext();) {
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
	public Composite addBytes(byte[] part) {
		initOutputStream();
		try {
			out.write(COMPONENT_BYTES);
			out.writeShort(part.length);
			out.write(part);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided ASCII string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as an ASCII string
	 * @return the composite type builder for chained invocation
	 */
	public Composite addAscii(String str) {
		initOutputStream();
		try {
			byte[] bytes = bytes(str, ASCII_ENCODING);
			out.write(COMPONENT_ASCII);
			out.writeShort(bytes.length);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided UTF8 string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as a UTF8 string
	 * @return the composite type builder for chained invocation
	 */
	public Composite addUTF8(String str) {
		initOutputStream();
		try {
			byte[] bytes = bytes(str);
			out.write(COMPONENT_UTF8);
			out.writeShort(bytes.length);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided boolean value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a boolean value
	 * @return the composite type builder for chained invocation
	 */
	public Composite addBool(boolean val) {
		initOutputStream();
		try {
			out.write(COMPONENT_BOOL);
			out.writeBoolean(val);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided long value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a long value
	 * @return the composite type builder for chained invocation
	 */
	public Composite addLong(long val) {
		initOutputStream();
		try {
			out.write(COMPONENT_LONG);
			out.writeLong(val);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the time-based UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a time-based UUID
	 * @return the composite type builder for chained invocation
	 */
	public Composite addTimeUUID(UUID uuid) {
		initOutputStream();
		try {
			byte[] bytes = bytes(uuid);
			out.write(COMPONENT_TIMEUUID);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the lexical UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a regular UUID
	 * @return the composite type builder for chained invocation
	 */
	public Composite addLexicalUUID(UUID uuid) {
		initOutputStream();
		try {
			byte[] bytes = bytes(uuid);
			out.write(COMPONENT_LEXICALUUID);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	public Composite addMatchMinimum() {
		initOutputStream();
		try {
			out.write(COMPONENT_MINIMUM);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	public Composite addMatchMaximum() {
		initOutputStream();
		try {
			out.write(COMPONENT_MAXIMUM);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	public void clear() {
		this.bytes = null;
		this.byteStream = null;
		this.out = null;
	}

	public boolean contains(Object o) {
		for (Iterator<? extends Object> iter = iterator(); iter.hasNext();) {
			Object obj = iter.next();
			if (obj.equals(o))
				return true;
		}
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		for (Iterator<? extends Object> iter = c.iterator(); iter.hasNext();) {
			Object obj = iter.next();
			if (!contains(obj))
				return false;
		}
		return true;
	}

	public boolean isEmpty() {
		pack();
		if ((bytes != null) && (bytes.length > 2))
			return false;
		return true;
	}

	public Iterator<Object> iterator() {
		pack();
		return new CompositeTypeIterator(this, bytes);
	}

	class CompositeTypeIterator implements Iterator<Object> {

		Composite collection;
		byte[] bytes;
		int offset = 0;
		int len = 0;
		int type = 0;

		CompositeTypeIterator(Composite c, byte[] bytes) {
			this.collection = c;
			this.bytes = bytes;
		}

		public boolean hasNext() {
			if (offset == 0) {
				if ((bytes == null) || (bytes.length == 0))
					return false;

				if ((bytes[0] & 0xFF) != COMPOSITETYPE_ID) {
					throw new MarshalException("Not a composite type");
				}

				if ((bytes[1] & 0xFF) != COMPOSITETYPE_VERSION) {
					throw new MarshalException(
							"Incorrect composite type version for this deserializer, expected "
									+ COMPOSITETYPE_VERSION + ", found "
									+ (bytes[1] & 0xFF));
				}
				offset = 2;
			}
			return offset < bytes.length;
		}

		public Object next() {
			if (!hasNext())
				throw new NoSuchElementException();
			int[] offsetRef = new int[1];
			offsetRef[0] = offset;
			Object obj = deserializeBytesAt(bytes, offsetRef);
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
		List<Object> objects = deserialize(bytes);
		return objects.size();
	}

	public Object[] toArray() {
		pack();
		List<Object> objects = deserialize(bytes);
		return objects.toArray();
	}

	public <T> T[] toArray(T[] a) {
		pack();
		List<Object> objects = deserialize(bytes);
		return objects.toArray(a);
	}

	public byte[] serialize() {
		pack();
		return bytes;
	}

	private static byte[] bytes(UUID uuid) {
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

	private static byte[] bytes(String s) {
		return bytes(s, UTF8_ENCODING);
	}

	private static byte[] bytes(String s, String encoding) {
		try {
			return s.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException ", e);
			throw new RuntimeException(e);
		}
	}

	public int compareTo(Composite o) {
		pack();
		return compare(serialize(), o.serialize());
	}

	public static int compare(Composite c1, Composite c2) {
		return compare(c1.serialize(), c2.serialize());
	}

	public static int compare(byte[] o1, byte[] o2) {

		if ((o1 == null) || (o1.length == 0)) {
			return ((o2 == null) || (o2.length == 0)) ? 0 : -1;
		}
		if ((o2 == null) || (o2.length == 0)) {
			return 1;
		}

		if (((o1[0] & 0xFF) != COMPOSITETYPE_ID)
				|| ((o2[0] & 0xFF) != COMPOSITETYPE_ID)) {
			throw new MarshalException("Not a composite type");
		}

		if (((o1[1] & 0xFF) != COMPOSITETYPE_VERSION)
				|| ((o2[1] & 0xFF) != COMPOSITETYPE_VERSION)) {
			throw new MarshalException(
					"Incorrect composite type version for this comparer");
		}

		int comp = 0;

		int offset1 = 2;
		int len1 = 0;
		int type1 = 0;

		int offset2 = 2;
		int len2 = 0;
		int type2 = 0;

		while (true) {
			type1 = o1[offset1] & 0xff;
			type2 = o2[offset2] & 0xff;

			if (type1 != type2) {
				return type1 < type2 ? -1 : 1;
			}

			switch (type1) {
			case COMPONENT_BYTES:
			case COMPONENT_ASCII:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareByteArrays(o1, offset1 + 3, len1, o2,
						offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COMPONENT_UTF8:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareUTF8(o1, offset1 + 3, len1, o2, offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COMPONENT_BOOL:
				comp = compareBool(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 2;
				offset2 += 2;
				break;

			case COMPONENT_LONG:
				comp = compareLong(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 9;
				offset2 += 9;
				break;

			case COMPONENT_LEXICALUUID:
				comp = compareLexicalUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

			case COMPONENT_TIMEUUID:
				comp = compareTimeUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

			case COMPONENT_MINIMUM:
			case COMPONENT_MAXIMUM:
				offset1 += 1;
				offset2 += 1;

			default:
				throw new MarshalException("Unknown embedded type: " + type1);
			}

			if (comp != 0)
				return comp;

			if (o1.length <= offset1) {
				return (o2.length <= offset2) ? 0 : -1;
			}
			if (o2.length <= offset2) {
				return 1;
			}

		}
	}

	private static int getShort(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xff) << 8) + (bytes[offset + 1] & 0xff);
	}

	private static int compareBool(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		boolean b1 = bytes1[offset1] != 0;
		boolean b2 = bytes2[offset2] != 0;
		if (!b1 && b2)
			return -1;
		if (b1 && !b2)
			return 1;
		return 0;
	}

	private static int compareLong(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		long L1 = ByteBuffer.wrap(bytes1, offset1, 8).getLong();
		long L2 = ByteBuffer.wrap(bytes2, offset2, 8).getLong();
		return Long.valueOf(L1).compareTo(Long.valueOf(L2));
	}

	private static String getHexString(byte[] bytes, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			int bint = bytes[i + offset] & 0xff;
			if (bint <= 0xF)
				// toHexString does not 0 pad its results.
				sb.append("0");
			sb.append(Integer.toHexString(bint));
		}
		return sb.toString();
	}

	private static int compareByteArrays(byte[] bytes1, int offset1, int len1,
			byte[] bytes2, int offset2, int len2) {
		if (null == bytes1) {
			if (null == bytes2)
				return 0;
			else
				return -1;
		}
		if (null == bytes2)
			return 1;

		if (len1 < 0)
			len1 = bytes1.length - offset1;
		if (len2 < 0)
			len2 = bytes2.length - offset2;

		int minLength = Math.min(len1, len2);
		for (int i = 0; i < minLength; i++) {
			int i1 = offset1 + i;
			int i2 = offset2 + i;
			if (bytes1[i1] == bytes2[i2])
				continue;
			// compare non-equal bytes as unsigned
			return (bytes1[i1] & 0xFF) < (bytes2[i2] & 0xFF) ? -1 : 1;
		}
		if (len1 == len2)
			return 0;
		else
			return (len1 < len2) ? -1 : 1;
	}

	private static int compareUTF8(byte[] bytes1, int offset1, int len1,
			byte[] bytes2, int offset2, int len2) {

		return new String(bytes1, offset1, len1).compareTo(new String(bytes2,
				offset2, len2));

	}

	private static UUID getUUID(byte[] bytes, int offset) {

		ByteBuffer bb = ByteBuffer.wrap(bytes, offset, 16);
		return new UUID(bb.getLong(), bb.getLong());

	}

	private static int compareLexicalUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		UUID uuid1 = getUUID(bytes1, offset1);
		UUID uuid2 = getUUID(bytes2, offset2);

		return uuid1.compareTo(uuid2);

	}

	private static long getUUIDTimestamp(byte[] bytes, int offset) {
		long low = 0;
		int mid = 0;
		int hi = 0;

		for (int i = 0; i < 4; i++)
			low = (low << 8) | (bytes[i + offset] & 0xff);
		for (int i = 4; i < 6; i++)
			mid = (mid << 8) | (bytes[i + offset] & 0xff);
		for (int i = 6; i < 8; i++)
			hi = (hi << 8) | (bytes[i + offset] & 0xff);

		return low + (mid << 32) + ((hi & 0x0FFF) << 48);
	}

	private static int compareTimeUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		long t1 = getUUIDTimestamp(bytes1, offset1);
		long t2 = getUUIDTimestamp(bytes2, offset2);

		return t1 < t2 ? -1 : (t1 > t2 ? 1 : compareByteArrays(bytes1, offset1,
				16, bytes2, offset2, 16));

	}

	private static Object deserializeBytesAt(byte[] bytes, int[] offsetRef) {
		Object result = null;
		int len = 0;
		int offset = offsetRef[0];
		int type = bytes[offset] & 0xff;
		// System.out.println(type);

		switch (type) {
		case COMPONENT_BYTES:
			len = getShort(bytes, offset + 1);
			byte[] b = new byte[len];
			System.arraycopy(bytes, offset + 3, b, 0, len);
			result = b;
			offset += len + 3;
			break;

		case COMPONENT_ASCII:
			len = getShort(bytes, offset + 1);
			try {
				result = new String(bytes, offset + 3, len, ASCII_ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			offset += len + 3;
			break;

		case COMPONENT_UTF8:
			len = getShort(bytes, offset + 1);
			try {
				result = new String(bytes, offset + 3, len, UTF8_ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			offset += len + 3;
			break;

		case COMPONENT_LONG:
			result = ByteBuffer.wrap(bytes, offset + 1, 8).getLong();
			offset += 9;
			break;

		case COMPONENT_LEXICALUUID:
		case COMPONENT_TIMEUUID:
			result = getUUID(bytes, offset + 1);
			offset += 17;
			break;

		case COMPONENT_BOOL:
			result = Boolean.valueOf(bytes[offset + 1] != 0);
			offset += 2;
			break;

		case COMPONENT_MINIMUM:
			result = Long.MIN_VALUE;
			offset += 1;
			break;

		case COMPONENT_MAXIMUM:
			result = Long.MAX_VALUE;
			offset += 1;
			break;

		default:
			throw new MarshalException("Unknown embedded type at offset: "
					+ offset);
		}

		offsetRef[0] = offset;
		return result;

	}

	public static List<Object> deserialize(byte[] bytes) {
		// System.out.println("column name bytes: "
		// + getBytesString(bytes, 0, bytes.length));
		List<Object> results = new ArrayList<Object>();

		if ((bytes == null) || (bytes.length == 0))
			return results;

		if ((bytes[0] & 0xFF) != COMPOSITETYPE_ID) {
			throw new MarshalException("Not a composite type");
		}

		if ((bytes[1] & 0xFF) != COMPOSITETYPE_VERSION) {
			throw new MarshalException(
					"Incorrect composite type version for this deserializer, expected "
							+ COMPOSITETYPE_VERSION + ", found "
							+ (bytes[1] & 0xFF));
		}

		int offset = 2;
		int[] offsetRef = new int[1];

		while (true) {
			offsetRef[0] = offset;
			results.add(deserializeBytesAt(bytes, offsetRef));
			offset = offsetRef[0];

			if (bytes.length <= offset) {
				break;
			}

		}

		return results;
	}

	public static boolean validate(byte[] bytes) {
		if ((bytes != null) && (bytes.length > 0)) {
			if ((bytes[0] & 0xFF) != COMPOSITETYPE_ID) {
				return false;
			}

			if ((bytes[1] & 0xFF) != COMPOSITETYPE_VERSION) {
				return false;
			}
		}
		return true;
	}

	public static byte[] serialize(Object... objects) {
		Composite c = new Composite(objects);
		return c.serialize();
	}

	private static boolean isTimeBased(UUID uuid) {
		try {
			uuid.timestamp();
			return true;
		} catch (UnsupportedOperationException e) {
		}
		return false;
	}
}
