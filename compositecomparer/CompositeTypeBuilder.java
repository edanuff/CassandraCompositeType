package compositecomparer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * CompositeTypeBuilder is a utility class for creating composite types for the
 * CompositeType comparer.
 * <p>
 * Internally, this class constructs a ByteArrayOutputStream with the appended
 * data for each component type.  It's possible, although very unlikely, that
 * an IOException can be thrown while adding a component type.  If this occurs
 * in production code, it likely represents an out of memory condition.  In any
 * case, it goes without saying that any column name generated if an exception
 * is thrown should not be used to proceed with an insert.
 */
public class CompositeTypeBuilder {

	ByteArrayOutputStream byteStream;

	DataOutputStream out;

	/**
	 * Default constructor.
	 */
	public CompositeTypeBuilder() {
		byteStream = new ByteArrayOutputStream();
		out = new DataOutputStream(byteStream);
	}

	/**
	 * Gets the contents of the composite type as a byte array usable as a
	 * column name.
	 * 
	 * @return a byte array of the combined component parts
	 */
	public byte[] getBytes() {
		return byteStream.toByteArray();
	}

	/**
	 * Adds the provided byte array to the composite type being built.
	 * 
	 * @param part
	 *            the component part to append as a byte array
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeBuilder addBytes(byte[] part) throws IOException {
		out.write(CompositeType.COLUMNTYPE_BYTES);
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
	public CompositeTypeBuilder addAscii(String str) throws IOException {
		byte[] bytes = CompositeTypeUtils.bytes(str,
				CompositeType.ASCII_ENCODING);
		out.write(CompositeType.COLUMNTYPE_ASCII);
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
	public CompositeTypeBuilder addUTF8(String str) throws IOException {
		byte[] bytes = CompositeTypeUtils.bytes(str);
		out.write(CompositeType.COLUMNTYPE_UTF8);
		out.writeShort(bytes.length);
		out.write(bytes);
		return this;
	}

	/**
	 * Adds the provided long value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a long value
	 * @return the composite type builder for chained invocation
	 */
	public CompositeTypeBuilder addLong(long val) throws IOException {
		out.write(CompositeType.COLUMNTYPE_LONG);
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
	public CompositeTypeBuilder addTimeUUID(java.util.UUID uuid)
			throws IOException {
		byte[] bytes = CompositeTypeUtils.bytes(uuid);
		out.write(CompositeType.COLUMNTYPE_TIMEUUID);
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
	public CompositeTypeBuilder addLexicalUUID(java.util.UUID uuid)
			throws IOException {
		byte[] bytes = CompositeTypeUtils.bytes(uuid);
		out.write(CompositeType.COLUMNTYPE_LEXICALUUID);
		out.write(bytes);
		return this;
	}
}
