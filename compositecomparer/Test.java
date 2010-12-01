package compositecomparer;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.cassandra.utils.FBUtilities;

public class Test {

	private static final Logger logger = Logger.getLogger(Test.class.getName());

	public static java.util.UUID getTimeUUID() {
		return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
	}

	public static void logCompare(CompositeType comparer, ByteBuffer o1,
			ByteBuffer o2) {
		int c = comparer.compare(o1, o2);
		logger.info(comparer.getString(o1)
				+ (c > 0 ? " > " : c < 0 ? " < " : " = ")
				+ comparer.getString(o2));
	}

	public static void main(String[] args) {

		CompositeType comparer = new CompositeType();

		// Create and serialize composite value

		Composite c = new Composite("smith", "bob", System.currentTimeMillis());
		ByteBuffer o1 = c.serializeToByteBuffer();
		logger.info(c.toString());

		logger.info(comparer.getString(o1) + " is encoded as "
				+ FBUtilities.bytesToHex(o1));

		// Deserialize composite value

		c = new Composite(o1);
		logger.info(c.toString());

		// Test comparisons

		c = new Composite();
		c.addAscii("hello").addLong(256);
		ByteBuffer o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		UUID u1 = getTimeUUID();

		c = new Composite();
		c.addTimeUUID(u1).addLong(256);
		o1 = c.serializeToByteBuffer();

		u1 = getTimeUUID();
		c = new Composite();
		c.addTimeUUID(u1).addLong(256);
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		c = new Composite(256);
		o1 = c.serializeToByteBuffer();

		c = new Composite();
		c.addLong(256);
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		c = new Composite(256);
		o1 = c.serializeToByteBuffer();

		c = new Composite(256, Composite.MATCH_MINIMUM);
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		c = new Composite(256, Composite.MATCH_MINIMUM);
		o1 = c.serializeToByteBuffer();

		c = new Composite(256, 0);
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		c = new Composite(256, 10);
		o1 = c.serializeToByteBuffer();

		c = new Composite(256, Composite.MATCH_MAXIMUM);
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

		c = new Composite("alpha");
		o1 = c.serializeToByteBuffer();

		c = new Composite("beta");
		o2 = c.serializeToByteBuffer();

		logCompare(comparer, o1, o2);

	}

}
