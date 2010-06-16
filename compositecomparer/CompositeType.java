package compositecomparer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;

/**
 * CompositeType comparer for Cassandra columns.
 * <p>
 * CompositeType is a comparer that allows you to combine the existing Cassandra
 * types into a composite type that will then be compared correctly for each of
 * the component types.
 * <p>
 * To use this, you must specify the comparer in your storage-conf.xml file:
 * <p>
 * <code>
 * &lt;ColumnFamily CompareWith="compositecomparer.CompositeType"
 *        Name="Stuff"/>
 * </code>
 * <p>
 * To construct a composite name for a new column, use the following:
 * <p>
 * <code>
 * CompositeTypeBuilder builder = new CompositeTypeBuilder();<br>
 * builder.addUTF8("smith").addUTF8("bob").addLexicalUUID(new UUID());<br>
 * byte[] bytes = builder.getBytes();<br>
 * </code>
 * <p>
 * The actual composite type consists of 1 byte to specify the component type,
 * and then for variable length types such as ASCII strings, 2 bytes are used
 * for each string length.
 * 
 */

public class CompositeType extends AbstractType {

	static final Logger logger = Logger
			.getLogger(CompositeType.class.getName());

	public final static int COLUMNTYPE_LONG = 0;

	public final static int COLUMNTYPE_BYTES = 1;

	public final static int COLUMNTYPE_ASCII = 2;

	public final static int COLUMNTYPE_UTF8 = 3;

	public final static int COLUMNTYPE_LEXICALUUID = 4;

	public final static int COLUMNTYPE_TIMEUUID = 5;

	public final static int COLUMNTYPE_MINIMUM = 16;
	public final static int COLUMNTYPE_MAXIMUM = 17;

	public static final String UTF8_ENCODING = "UTF-8";

	public static final String ASCII_ENCODING = "US-ASCII";

	@Override
	public String getString(byte[] bytes) {
		return columnNameToString(bytes);
	}

	public static String columnNameToString(byte[] bytes) {
		//System.out.println("column name bytes: "
		//		+ getBytesString(bytes, 0, bytes.length));

		if ((bytes == null) || (bytes.length == 0))
			return "";

		StringBuilder result = new StringBuilder();

		int offset = 0;
		int len = 0;
		int type = 0;

		while (true) {
			type = bytes[offset] & 0xff;
			//System.out.println(type);

			switch (type) {
			case COLUMNTYPE_BYTES:
				len = getShort(bytes, offset + 1);
				//System.out.println(len);
				result.append(getBytesString(bytes, offset + 3, len));
				offset += len + 3;
				break;

			case COLUMNTYPE_ASCII:
				len = getShort(bytes, offset + 1);
				//System.out.println(len);
				result.append(getAsciiString(bytes, offset + 3, len));
				offset += len + 3;
				break;

			case COLUMNTYPE_UTF8:
				len = getShort(bytes, offset + 1);
				//System.out.println(len);
				result.append(getUTF8String(bytes, offset + 3, len));
				offset += len + 3;
				break;

			case COLUMNTYPE_LONG:
				result.append(getLongString(bytes, offset + 1));
				offset += 9;
				break;

			case COLUMNTYPE_LEXICALUUID:
			case COLUMNTYPE_TIMEUUID:
				result.append(getUUIDString(bytes, offset + 1));
				offset += 17;
				break;

			case COLUMNTYPE_MINIMUM:
				result.append("MIN");
				offset += 1;
				break;

			case COLUMNTYPE_MAXIMUM:
				result.append("MAX");
				offset += 1;
				break;

			default:
				throw new MarshalException("Unknown embedded type at offset: "
						+ offset + " parsed " + result.toString());
			}

			if (bytes.length <= offset) {
				break;
			}

			result.append(",");
		}

		return result.toString();
	}

	public int compare(byte[] o1, byte[] o2) {

		if ((o1 == null) || (o1.length == 0)) {
			return ((o2 == null) || (o2.length == 0)) ? 0 : -1;
		}
		if ((o2 == null) || (o2.length == 0)) {
			return 1;
		}

		int comp = 0;

		int offset1 = 0;
		int len1 = 0;
		int type1 = 0;

		int offset2 = 0;
		int len2 = 0;
		int type2 = 0;

		while (true) {
			type1 = o1[offset1];
			type2 = o2[offset2];

			if (type1 == COLUMNTYPE_MINIMUM)
				return (type2 == COLUMNTYPE_MINIMUM) ? 0 : -1;
			if (type1 == COLUMNTYPE_MAXIMUM)
				return (type2 == COLUMNTYPE_MAXIMUM) ? 0 : 1;
			if (type2 == COLUMNTYPE_MINIMUM)
				return 1;
			if (type2 == COLUMNTYPE_MAXIMUM)
				return -1;

			if (type1 != type2) {
				return compareByteArrays(o1, offset1 + 1, -1, o2, offset2 + 1,
						-1);
			}

			switch (type1) {
			case COLUMNTYPE_BYTES:
			case COLUMNTYPE_ASCII:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareByteArrays(o1, offset1 + 3, len1, o2,
						offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COLUMNTYPE_UTF8:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareUTF8(o1, offset1 + 3, len1, o2, offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COLUMNTYPE_LONG:
				comp = compareLong(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 9;
				offset2 += 9;
				break;

			case COLUMNTYPE_LEXICALUUID:
				comp = compareLexicalUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

			case COLUMNTYPE_TIMEUUID:
				comp = compareTimeUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

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

	public static int getShort(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xff) << 8) + (bytes[offset + 1] & 0xff);
	}

	public static String getLongString(byte[] bytes, int offset) {
		return String.valueOf(ByteBuffer.wrap(bytes, offset, 8).getLong());
	}

	public static int compareLong(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		long L1 = ByteBuffer.wrap(bytes1, offset1, 8).getLong();
		long L2 = ByteBuffer.wrap(bytes2, offset2, 8).getLong();
		return Long.valueOf(L1).compareTo(Long.valueOf(L2));
	}

	public static String getBytesString(byte[] bytes, int offset, int len) {
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

	public static int compareByteArrays(byte[] bytes1, int offset1, int len1,
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

	public static String getAsciiString(byte[] bytes, int offset, int len) {
		try {
			return new String(bytes, offset, len, ASCII_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getUTF8String(byte[] bytes, int offset, int len) {
		try {
			return new String(bytes, offset, len, UTF8_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static int compareUTF8(byte[] bytes1, int offset1, int len1,
			byte[] bytes2, int offset2, int len2) {

		return new String(bytes1, offset1, len1).compareTo(new String(bytes2,
				offset2, len2));

	}

	public static UUID getUUID(byte[] bytes, int offset) {

		ByteBuffer bb = ByteBuffer.wrap(bytes, offset, 16);
		return new UUID(bb.getLong(), bb.getLong());

	}

	public static String getUUIDString(byte[] bytes, int offset) {
		return getUUID(bytes, offset).toString();
	}

	public static int compareLexicalUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		UUID uuid1 = getUUID(bytes1, offset1);
		UUID uuid2 = getUUID(bytes2, offset2);

		return uuid1.compareTo(uuid2);

	}

	public static long getUUIDTimestamp(byte[] bytes, int offset) {
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

	public static int compareTimeUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		long t1 = getUUIDTimestamp(bytes1, offset1);
		long t2 = getUUIDTimestamp(bytes2, offset2);

		return t1 < t2 ? -1 : (t1 > t2 ? 1 : compareByteArrays(bytes1, offset1,
				16, bytes2, offset2, 16));

	}

}
