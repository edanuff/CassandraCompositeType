CompositeType for Cassandra

Although Cassandra provides SuperColumns which allow you to have columns
containing columns, it's often desirable to use regular columns and to
be able to combine two or more ids into a sortable column name.  This
is especially important for building inverted indexes for searches or
being able to do things like creating a table to map user names sorted
by last name then first name to user ids.

This can accomplished by concatenating two ids together with a separator,
such as "last.first.randomuuid", and assuming that each id is properly
passed and compares correctly via bytewise comparison, this approach might
work.  But, if the ids are numerical or time-based UUIDs for example, the
results are usually less than satisfactory.

CompositeType is a comparer that allows you to combine the existing
Cassandra types into a composite type that will then be compared correctly
for each of the component types.

To use this, you must specify the comparer in your storage-conf.xml file:

<ColumnFamily CompareWith="compositecomparer.CompositeType" Name="Stuff"/>

To construct a composite name for a new column, use the following:

CompositeTypeBuilder builder = new CompositeTypeBuilder();
builder.addUTF8("smith").addUTF8("bob").addLong(System.currentTimeMillis());
byte[] column_name = builder.getBytes();

A convenience method is provided as well, although it makes certain assumptions
that you might want to very are applicable.  You use it like this:

import static compositecomparer.CompositeTypeBuilder.composite;

byte[] cname = composite("smith", "bob", new Long(System.currentTimeMillis()));

If you wanted to find all users with the last name "smith" whose name started
with "b", you could do the following:

byte[] slice_start = composite("smith", "b\u0000");
byte[] slice_end = composite("smith", "b\uFFFF");

The composite type is encoded as a byte array consisting of a prefix byte and a
version byte followed by each component part.  Each component part starts with
1 byte to specify the component type, and then for variable length types such
as ASCII strings, 2 bytes are used for the length of the string:

In the above example, the following byte array would be produced for the composite
name:

ed 01 05 00 05 73 6d 69 74 68 05 00 03 62 6f 62 01 00 00 01 29 47 2f 24 e0

