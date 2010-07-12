package compositecomparer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CompositeTypeBuilder is a utility class for creating composite types for the
 * CompositeType comparer.
 * <p>
 * Internally, this class constructs a ByteArrayOutputStream with the appended
 * data for each component type. It's possible, although very unlikely, that an
 * IOException can be thrown while adding a component type. If this occurs in
 * production code, it likely represents an out of memory condition. In any
 * case, it goes without saying that any column name generated if an exception
 * is thrown should not be used to proceed with an insert.
 */
public class CompositeTypeBuilder {

	static final Logger logger = Logger.getLogger(CompositeTypeBuilder.class
			.getName());

	ByteArrayOutputStream byteStream;

	DataOutputStream out;

	/**
	 * Default constructor.
	 */
	public CompositeTypeBuilder() throws IOException {
		byteStream = new ByteArrayOutputStream();
		out = new DataOutputStream(byteStream);
		out.write(CompositeTypeUtils.COMPOSITETYPE_ID);
		out.write(CompositeTypeUtils.COMPOSITETYPE_VERSION);
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
	public CompositeTypeBuilder addAscii(String str) throws IOException {
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
	public CompositeTypeBuilder addUTF8(String str) throws IOException {
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
	public CompositeTypeBuilder addBool(boolean val) throws IOException {
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
	public CompositeTypeBuilder addLong(long val) throws IOException {
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
	public CompositeTypeBuilder addTimeUUID(UUID uuid) throws IOException {
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
	public CompositeTypeBuilder addLexicalUUID(UUID uuid) throws IOException {
		byte[] bytes = bytes(uuid);
		out.write(CompositeTypeUtils.COMPONENT_LEXICALUUID);
		out.write(bytes);
		return this;
	}

	public CompositeTypeBuilder addMatchMinimum() throws IOException {
		out.write(CompositeTypeUtils.COMPONENT_MINIMUM);
		return this;
	}

	public CompositeTypeBuilder addMatchMaximum() throws IOException {
		out.write(CompositeTypeUtils.COMPONENT_MAXIMUM);
		return this;
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
