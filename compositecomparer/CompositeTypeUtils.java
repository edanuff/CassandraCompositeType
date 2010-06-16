package compositecomparer;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CompositeTypeUtils provides utility methods common to both CompositeType and
 * CompositeTypeBuilder.
 */
public class CompositeTypeUtils {

	static final Logger logger = Logger.getLogger(CompositeTypeUtils.class
			.getName());

	public static UUID uuid(byte[] uuid) {
		long msb = 0;
		long lsb = 0;
		assert uuid.length == 16;
		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (uuid[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (uuid[i] & 0xff);
	
		UUID u = new UUID(msb, lsb);
		return UUID.fromString(u.toString());
	}

	public static byte[] bytes(UUID uuid) {
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

	public static byte[] bytes(String s) {
		return CompositeTypeUtils.bytes(s, CompositeType.UTF8_ENCODING);
	}

	public static byte[] bytes(String s, String encoding) {
		try {
			return s.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException ", e);
			throw new RuntimeException(e);
		}
	}

	public static String string(byte[] bytes) {
		return CompositeTypeUtils.string(bytes, CompositeType.UTF8_ENCODING);
	}

	public static String string(byte[] bytes, String encoding) {
		if (bytes == null) {
			return null;
		}
		try {
			return new String(bytes, encoding);
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException ", e);
			throw new RuntimeException(e);
		}
	}
	
	public static boolean isTimeBased(UUID uuid) {
		try {
			uuid.timestamp();
			return true;
		}
		catch (UnsupportedOperationException e) {
		}
		return false;
	}

}
