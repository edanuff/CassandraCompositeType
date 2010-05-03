CompositeType for Cassandra

Although Cassandra provides SuperColumns which allow you to have columns
containing columns, it's often desirable to use regular columns and to
be able to combine two or more ids into a sortable column name.

This can accomplished by concatenating two ids together with a seperator,
such as "username.groupname.propertyname", and in many cases, simple
bytewise comparisons will work, but if the individual parts are numeric
or the first part is a lexical uuid and the second one is a time uuid,
this approach won't work.

CompositeType is a comparer that allows you to combine the existing
Cassandra types into a composite type that will then be compared correctly
for each of the component types.

To use this, you must specify the comparer in your storage-conf.xml file:

<ColumnFamily CompareWith="compositecomparer.CompositeType"
       Name="Stuff"/>

To construct a composite name for a new column, use the following:

CompositeTypeBuilder builder = new CompositeTypeBuilder();
builder.addAscii("hello").addLong(255);
byte[] bytes = builder.getBytes();

The actual composite type consists of 1 byte to specify the
component type, and then for variable length types such as ASCII strings,
2 bytes are used for each string length.
