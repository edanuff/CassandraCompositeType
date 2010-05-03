package compositecomparer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * CompositeType comparer for Cassandra columns.
 * <p>
 * CompositeType is a comparer that allows you to combine the existing
 * Cassandra types into a composite type that will then be compared correctly
 * for each of the component types.
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
 * builder.addAscii("hello").addLong(255);<br>
 * byte[] bytes = builder.getBytes();<br>
 * </code>
 * <p>
 * The actual composite type consists of 1 byte to specify the
 * component type, and then for variable length types such as ASCII strings,
 * 2 bytes are used for each string length.
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

	public static final String UTF8_ENCODING = "UTF-8";
	public static final String ASCII_ENCODING = "US-ASCII";

	@Override
	public String getString(byte[] bytes) {
		StringBuilder result = new StringBuilder();

		try {
			CompositeColumnName n = new CompositeColumnName(bytes);
			CompositeColumnPart p = n.getNextColumnPart();
			while (p != null) {
				result.append(p.marshaller.getString(p.bytes));
				p = n.getNextColumnPart();
				if (p != null)
					result.append('.');
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error during compare", e);
		}

		return result.toString();
	}

	public int compare(byte[] o1, byte[] o2) {

		if (null == o1) {
			if (null == o2)
				return 0;
			else
				return -1;
		}
		if (null == o2)
			return 1;

		int comp = 0;

		try {

			CompositeColumnName n1 = new CompositeColumnName(o1);
			CompositeColumnName n2 = new CompositeColumnName(o2);

			CompositeColumnPart p1 = n1.getNextColumnPart();
			CompositeColumnPart p2 = n2.getNextColumnPart();

			while (comp == 0) {

				if (null == p1) {
					if (null == p2)
						return 0;
					else
						return -1;
				}
				if (null == p2)
					return 1;

				comp = p1.marshaller.compare(p1.bytes, p2.bytes);

				p1 = n1.getNextColumnPart();
				p2 = n2.getNextColumnPart();
			}

		} catch (IOException e) {
			logger.log(Level.SEVERE, "Unexpected IOException during compare", e);
		}

		return comp;
	}

	public class CompositeColumnPart {
		AbstractType marshaller;
		byte[] bytes;
	}

	public class CompositeColumnName {

		ByteArrayInputStream byteStream;
		DataInputStream in;

		public CompositeColumnName(byte[] bytes) {
			byteStream = new ByteArrayInputStream(bytes);
			in = new DataInputStream(byteStream);
		}

		public CompositeColumnPart getNextColumnPart() throws IOException {
			CompositeColumnPart part = new CompositeColumnPart();
			
			int type = in.read();
			if (type == -1)
				return null;
			
			int l = 0;
			
			switch (type) {
			case COLUMNTYPE_BYTES:
				l = in.readShort();
				part.bytes = new byte[l];
				in.readFully(part.bytes);
				part.marshaller = new BytesType();
				return part;

			case COLUMNTYPE_ASCII:
				l = in.readShort();
				part.bytes = new byte[l];
				in.readFully(part.bytes);
				part.marshaller = new AsciiType();
				return part;

			case COLUMNTYPE_UTF8:
				l = in.readShort();
				part.bytes = new byte[l];
				in.readFully(part.bytes);
				part.marshaller = new UTF8Type();
				return part;

			case COLUMNTYPE_LONG:
				part.bytes = new byte[8];
				in.readFully(part.bytes);
				part.marshaller = new LongType();
				return part;

			case COLUMNTYPE_LEXICALUUID:
				part.bytes = new byte[16];
				in.readFully(part.bytes);
				part.marshaller = new LexicalUUIDType();
				return part;

			case COLUMNTYPE_TIMEUUID:
				part.bytes = new byte[16];
				in.readFully(part.bytes);
				part.marshaller = new TimeUUIDType();
				return part;

			default:
				break;
			}

			return null;
		}
	}

}
