CompositeType for Cassandra

For a more detailed description of how this might be used, please read:

http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html

For an example implemention of using it for indexing, see:

https://github.com/edanuff/CassandraIndexedCollections

-------------------------------------------------------------------------------
Note:

Be aware that most of the capabilities provided by this comparer have
been incorporated in the proposed path described at
https://issues.apache.org/jira/browse/CASSANDRA-2231 and will hopefully
be present at some point at or after the release of 0.7.4.
Once that has been built into Cassandra, the recommended approach will
be to use the new DynamicCompositeType comparer rather than this one.

The index-building techniques described here will still be applicable
though and should be used in place of SuperColumn-based approaches and,
once support for composite-based indexes are incorporated into the major
client libraries, will often be preferable to the built-in secondary
indexes as well.
-------------------------------------------------------------------------------

Although Cassandra provides SuperColumns which allow you to have columns
containing columns, it's often desirable to use regular columns and to
be able to combine two or more ids into a sortable column name.  This
is especially important for building inverted indexes for searches or
being able to do things like creating a table to map user names sorted
by last name then first name to user ids.

This can accomplished by concatenating two ids together with a separator,
such as "last.first.randomuuid" or "item.version", and assuming that each
id is properly passed and compares correctly via bytewise comparison, this
approach might work.  But, if the ids are numerical or time-based UUIDs for
example, the results are usually less than satisfactory.

CompositeType is a comparer that allows you to combine the existing
Cassandra types into a composite type that will then be compared correctly
for each of the component types.

To use this, you must specify the comparer in your cassandra.yaml file:

  column_families:
    - name: Stuff
      compare_with: compositecomparer.CompositeType

To construct a composite name for a new column, use the following:

Composite c = new Composite();
c.addUTF8("smith").addUTF8("bob").addLong(System.currentTimeMillis());
ByteBuffer column_name = c.serializeToByteBuffer();

A convenience method is provided as well, although it makes certain assumptions
that you might want to verify are applicable.  You use it like this:

import static compositecomparer.Composite.serialize;

byte[] cname = serialize("smith", "bob", new Long(System.currentTimeMillis()));

If you wanted to find all users with the last name "smith" whose name started
with "b", you could do the following:

byte[] slice_start = serialize("smith", "b");
byte[] slice_end = serialize("smith", "b\uFFFF");

This has also been updated to work with ByteBuffers for Cassandra 7.0:

import static compositecomparer.Composite.serializeToByteBuffer;

ByteBuffer cname = serializeToByteBuffer("document", version);

The composite type is encoded as a byte array consisting of a four byte prefix
containing an identifier and a version number, followed by each component part.
Each component part starts with 1 byte to specify the component type, and then
for variable length types such as ASCII strings, 2 bytes are used for the length
of the string.
