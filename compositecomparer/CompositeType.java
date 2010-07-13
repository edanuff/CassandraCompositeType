package compositecomparer;

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

	static final Logger logger = Logger
			.getLogger(CompositeType.class.getName());

	@Override
	public void validate(byte[] bytes) {
		if (!Composite.validate(bytes)) {
			throw new MarshalException("Not a composite type or incorrect composite type version");
		}
	}

	@Override
	public String getString(byte[] bytes) {
		Composite c = new Composite(bytes);
		return c.toString();
	}

	public int compare(byte[] o1, byte[] o2) {
		return Composite.compare(o1, o2);
	}

}
