package compositecomparer;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.AbstractType;

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

public class CompositeType extends AbstractType {

	/**
	 * 
	 */
	public static final CompositeType instance = new CompositeType();

	/**
	 * 
	 */
	public CompositeType() {
	}

	static final Logger logger = Logger
			.getLogger(CompositeType.class.getName());

	@Override
	public void validate(ByteBuffer buffer) {
		Composite.validate(buffer, true);
	}

	@Override
	public String getString(ByteBuffer buffer) {
		Composite c = new Composite(buffer);
		return c.toString();
	}

	@Override
	public int compare(ByteBuffer b1, ByteBuffer b2) {
		return Composite.compare(b1, b2);
	}

}
