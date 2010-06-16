package compositecomparer;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.utils.FBUtilities;

public class Test {

	private static final Logger logger = Logger.getLogger(Test.class.getName());

	public static java.util.UUID getTimeUUID() {
		return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
	}

	public static void logCompare(CompositeType comparer, byte[] o1, byte[] o2) {
		int c = comparer.compare(o1, o2);
		logger.info(comparer.getString(o1)
				+ (c > 0 ? " > " : c < 0 ? " < " : " = ")
				+ comparer.getString(o2));
	}

	public static void main(String[] args) {

		try {
			CompositeType comparer = new CompositeType();

			CompositeTypeBuilder builder = new CompositeTypeBuilder();
			builder.addUTF8("smith").addUTF8("bob").addLong(System.currentTimeMillis());
			byte[] o1 = builder.getBytes();
			
			logger.info(comparer.getString(o1) + " is encoded as " + FBUtilities.bytesToHex(o1));

			builder = new CompositeTypeBuilder();
			builder.addAscii("hello").addLong(256);
			byte[] o2 = builder.getBytes();

			logCompare(comparer, o1, o2);

			UUID u1 = getTimeUUID();

			builder = new CompositeTypeBuilder();
			builder.addTimeUUID(u1).addLong(256);
			o1 = builder.getBytes();

			u1 = getTimeUUID();
			builder = new CompositeTypeBuilder();
			builder.addTimeUUID(u1).addLong(256);
			o2 = builder.getBytes();

			logCompare(comparer, o1, o2);

			builder = new CompositeTypeBuilder();
			builder.addLong(256);
			o1 = builder.getBytes();

			builder = new CompositeTypeBuilder();
			builder.addLong(256);
			o2 = builder.getBytes();

			logCompare(comparer, o1, o2);

		} catch (IOException e) {
			logger.log(Level.SEVERE,
					"IOException while constucting composite type", e);
		}

	}

}
