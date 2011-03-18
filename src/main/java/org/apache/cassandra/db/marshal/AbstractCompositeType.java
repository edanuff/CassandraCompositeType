package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;

/**
 * A class avoiding class duplication between CompositeType and
 * DynamicCompositeType. Those two differs only in that for
 * DynamicCompositeType, the comparators are in the encoded column name at the
 * front of each component.
 */
public abstract class AbstractCompositeType extends AbstractType {
	// changes bb position
	protected static int getShortLength(ByteBuffer bb) {
		int length = (bb.get() & 0xFF) << 8;
		return length | (bb.get() & 0xFF);
	}

	// changes bb position
	protected static void putShortLength(ByteBuffer bb, int length) {
		bb.put((byte) ((length >> 8) & 0xFF));
		bb.put((byte) (length & 0xFF));
	}

	// changes bb position
	protected static ByteBuffer getBytes(ByteBuffer bb, int length) {
		ByteBuffer copy = bb.duplicate();
		copy.limit(copy.position() + length);
		bb.position(bb.position() + length);
		return copy;
	}

	// changes bb position
	protected static ByteBuffer getWithShortLength(ByteBuffer bb) {
		int length = getShortLength(bb);
		return getBytes(bb, length);
	}

	@Override
	public int compare(ByteBuffer o1, ByteBuffer o2) {
		if (null == o1) {
			return null == o2 ? 0 : -1;
		}

		ByteBuffer bb1 = o1.duplicate();
		ByteBuffer bb2 = o2.duplicate();
		int i = 0;

		while ((bb1.remaining() > 0) && (bb2.remaining() > 0)) {
			AbstractType comparator = getNextComparator(i, bb1, bb2);

			ByteBuffer value1 = getWithShortLength(bb1);
			ByteBuffer value2 = getWithShortLength(bb2);

			int cmp = comparator.compare(value1, value2);
			if (cmp != 0) {
				return cmp;
			}

			byte b1 = bb1.get();
			byte b2 = bb2.get();
			if (b1 != 0) {
				return b2 != 0 ? 0 : 1;
			}
			if (b2 != 0) {
				return -1;
			}

			++i;
		}

		if (bb1.remaining() == 0) {
			return bb2.remaining() == 0 ? 0 : -1;
		}

		// bb1.remaining() > 0 && bb2.remaining() == 0
		return 1;
	}

	@Override
	public String getString(ByteBuffer bytes) {
		StringBuilder sb = new StringBuilder();
		ByteBuffer bb = bytes.duplicate();
		int i = 0;

		while (bb.remaining() > 0) {
			if (bb.remaining() != bytes.remaining()) {
				sb.append(":");
			}

			AbstractType comparator = getAndAppendNextComparator(i, bb, sb);
			ByteBuffer value = getWithShortLength(bb);

			sb.append(comparator.getString(value));

			byte b = bb.get();
			if (b != 0) {
				sb.append(":!");
				break;
			}
			++i;
		}
		return sb.toString();
	}

	/*
	 * FIXME: this would break if some of the component string representation
	 * contains ':'. None of our current comparator do so, so this is probably
	 * not an urgent matter, but this could break for custom comparator.
	 * (DynamicCompositeType would break on '@' too)
	 */
	@Override
	public ByteBuffer fromString(String source) {
		String[] parts = source.split(":");
		List<ByteBuffer> components = new ArrayList<ByteBuffer>();
		List<ParsedComparator> comparators = new ArrayList<ParsedComparator>();
		int totalLength = 0, i = 0;
		boolean lastByteIsOne = false;

		for (String part : parts) {
			if (part.equals("!")) {
				lastByteIsOne = true;
				break;
			}

			ParsedComparator p = parseNextComparator(i, part);
			AbstractType type = p.getAbstractType();
			part = p.getRemainingPart();

			ByteBuffer component = type.fromString(part);
			totalLength += p.getComparatorSerializedSize() + 2
					+ component.remaining() + 1;
			components.add(component);
			comparators.add(p);
			++i;
		}

		ByteBuffer bb = ByteBuffer.allocate(totalLength);
		i = 0;
		for (ByteBuffer component : components) {
			comparators.get(i).serializeComparator(bb);
			putShortLength(bb, component.remaining());
			bb.put(component); // it's ok to consume component as we won't use
								// it anymore
			bb.put((byte) 0);
			++i;
		}
		if (lastByteIsOne) {
			bb.put(bb.limit() - 1, (byte) 1);
		}

		bb.rewind();
		return bb;
	}

	@Override
	public void validate(ByteBuffer bytes) throws MarshalException {
		ByteBuffer bb = bytes.duplicate();

		int i = 0;
		while (bb.remaining() > 0) {
			AbstractType comparator = validateNextComparator(i, bb);

			if (bb.remaining() < 2) {
				throw new MarshalException(
						"Not enough bytes to read value size of component " + i);
			}
			int length = getShortLength(bb);

			if (bb.remaining() < length) {
				throw new MarshalException(
						"Not enough bytes to read value of component " + i);
			}
			ByteBuffer value = getBytes(bb, length);

			comparator.validate(value);

			if (bb.remaining() == 0) {
				throw new MarshalException(
						"Not enough bytes to read the end-of-component byte of component"
								+ i);
			}
			byte b = bb.get();
			if ((b != 0) && (bb.remaining() != 0)) {
				throw new MarshalException(
						"Invalid bytes remaining after an end-of-component at component"
								+ i);
			}
			++i;
		}
	}

	abstract protected AbstractType getNextComparator(int i, ByteBuffer bb);

	abstract protected AbstractType getNextComparator(int i, ByteBuffer bb1,
			ByteBuffer bb2);

	abstract protected AbstractType getAndAppendNextComparator(int i,
			ByteBuffer bb, StringBuilder sb);

	abstract protected ParsedComparator parseNextComparator(int i, String part);

	abstract protected AbstractType validateNextComparator(int i, ByteBuffer bb)
			throws MarshalException;

	protected static interface ParsedComparator {
		AbstractType getAbstractType();

		String getRemainingPart();

		int getComparatorSerializedSize();

		void serializeComparator(ByteBuffer bb);
	}
}
