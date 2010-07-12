package compositecomparer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.MarshalException;

/**
 * CompositeTypeUtils provides utility methods common to both CompositeType and
 * CompositeTypeBuilder.
 */
public class CompositeTypeUtils {

	static final Logger logger = Logger.getLogger(CompositeTypeUtils.class
			.getName());

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

	public static String getString(byte[] bytes) {

		// System.out.println("column name bytes: "
		// + getBytesString(bytes, 0, bytes.length));

		if ((bytes == null) || (bytes.length == 0))
			return "";

		if ((bytes[0] & 0xFF) != COMPOSITETYPE_ID) {
			throw new MarshalException("Not a composite type");
		}

		if ((bytes[1] & 0xFF) != COMPOSITETYPE_VERSION) {
			throw new MarshalException(
					"Incorrect composite type version for this deserializer, expected "
							+ COMPOSITETYPE_VERSION + ", found "
							+ (bytes[1] & 0xFF));
		}

		StringBuilder result = new StringBuilder();

		int offset = 2;
		int len = 0;
		int type = 0;

		while (true) {
			type = bytes[offset] & 0xff;
			// System.out.println(type);

			switch (type) {
			case COMPONENT_BYTES:
				len = getShort(bytes, offset + 1);
				// System.out.println(len);
				result.append(getHexString(bytes, offset + 3, len));
				offset += len + 3;
				break;

			case COMPONENT_ASCII:
				len = getShort(bytes, offset + 1);
				// System.out.println(len);
				try {
					result.append(new String(bytes, offset + 3, len,
							ASCII_ENCODING));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				offset += len + 3;
				break;

			case COMPONENT_UTF8:
				len = getShort(bytes, offset + 1);
				// System.out.println(len);
				try {
					result.append(new String(bytes, offset, len + 3,
							UTF8_ENCODING));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				offset += len + 3;
				break;

			case COMPONENT_BOOL:
				result.append(getBool(bytes, offset + 1));
				offset += 1;
				break;

			case COMPONENT_LONG:
				result.append(ByteBuffer.wrap(bytes, offset + 1, 8).getLong());
				offset += 9;
				break;

			case COMPONENT_LEXICALUUID:
			case COMPONENT_TIMEUUID:
				result.append(getUUID(bytes, offset + 1));
				offset += 17;
				break;

			case COMPONENT_MINIMUM:
				result.append("MIN");
				offset += 1;
				break;

			case COMPONENT_MAXIMUM:
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

	static boolean getBool(byte[] bytes, int offset) {
		return bytes[offset] != 0;
	}

	static int getShort(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xff) << 8) + (bytes[offset + 1] & 0xff);
	}

	static int compareBool(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		boolean b1 = bytes1[offset1] != 0;
		boolean b2 = bytes2[offset2] != 0;
		if (!b1 && b2)
			return -1;
		if (b1 && !b2)
			return 1;
		return 0;
	}

	static int compareLong(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		long L1 = ByteBuffer.wrap(bytes1, offset1, 8).getLong();
		long L2 = ByteBuffer.wrap(bytes2, offset2, 8).getLong();
		return Long.valueOf(L1).compareTo(Long.valueOf(L2));
	}

	static String getHexString(byte[] bytes, int offset, int len) {
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

	static int compareByteArrays(byte[] bytes1, int offset1, int len1,
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

	static int compareUTF8(byte[] bytes1, int offset1, int len1, byte[] bytes2,
			int offset2, int len2) {

		return new String(bytes1, offset1, len1).compareTo(new String(bytes2,
				offset2, len2));

	}

	static UUID getUUID(byte[] bytes, int offset) {

		ByteBuffer bb = ByteBuffer.wrap(bytes, offset, 16);
		return new UUID(bb.getLong(), bb.getLong());

	}

	static int compareLexicalUUID(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		UUID uuid1 = getUUID(bytes1, offset1);
		UUID uuid2 = getUUID(bytes2, offset2);

		return uuid1.compareTo(uuid2);

	}

	static long getUUIDTimestamp(byte[] bytes, int offset) {
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

	static int compareTimeUUID(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		long t1 = getUUIDTimestamp(bytes1, offset1);
		long t2 = getUUIDTimestamp(bytes2, offset2);

		return t1 < t2 ? -1 : (t1 > t2 ? 1 : compareByteArrays(bytes1, offset1,
				16, bytes2, offset2, 16));

	}

	public static Object[] deserialize(byte[] bytes) {
		// System.out.println("column name bytes: "
		// + getBytesString(bytes, 0, bytes.length));
		List<Object> results = new ArrayList<Object>();

		if ((bytes == null) || (bytes.length == 0))
			return results.toArray();

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
		int len = 0;
		int type = 0;

		while (true) {
			type = bytes[offset] & 0xff;
			// System.out.println(type);

			switch (type) {
			case COMPONENT_BYTES:
				len = getShort(bytes, offset + 1);
				results.add(getHexString(bytes, offset + 3, len));
				offset += len + 3;
				break;

			case COMPONENT_ASCII:
				len = getShort(bytes, offset + 1);
				try {
					results.add(new String(bytes, offset + 3, len,
							ASCII_ENCODING));
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
				offset += len + 3;
				break;

			case COMPONENT_UTF8:
				len = getShort(bytes, offset + 1);
				try {
					results.add(new String(bytes, offset + 3, len,
							UTF8_ENCODING));
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
				offset += len + 3;
				break;

			case COMPONENT_LONG:
				results.add(ByteBuffer.wrap(bytes, offset + 1, 8).getLong());
				offset += 9;
				break;

			case COMPONENT_LEXICALUUID:
			case COMPONENT_TIMEUUID:
				results.add(getUUID(bytes, offset + 1));
				offset += 17;
				break;

			case COMPONENT_MINIMUM:
				results.add(Integer.MIN_VALUE);
				offset += 1;
				break;

			case COMPONENT_MAXIMUM:
				results.add(Integer.MAX_VALUE);
				offset += 1;
				break;

			default:
				throw new MarshalException("Unknown embedded type at offset: "
						+ offset + " parsed " + results.size() + " components");
			}

			if (bytes.length <= offset) {
				break;
			}

		}

		return results.toArray();
	}

	public static byte[] serialize(Object... objects) throws IOException {
		CompositeTypeBuilder builder = new CompositeTypeBuilder();
		for (Object obj : objects) {
			if (obj instanceof Long)
				builder.addLong(((Long) obj).longValue());
			else if (obj instanceof Boolean) {
				builder.addBool(((Boolean) obj).booleanValue());
			}
			else if (obj instanceof String) {
				builder.addUTF8((String) obj);
			}
			else if (obj instanceof UUID) {
				if (isTimeBased((UUID) obj)) {
					builder.addTimeUUID((UUID) obj);
				} else {
					builder.addLexicalUUID((UUID) obj);
				}
			} else if (obj instanceof byte[]) {
				builder.addBytes((byte[]) obj);
			}
		}
		return builder.getBytes();
	}

	static boolean isTimeBased(UUID uuid) {
		try {
			uuid.timestamp();
			return true;
		} catch (UnsupportedOperationException e) {
		}
		return false;
	}

}
