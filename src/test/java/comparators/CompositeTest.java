package comparators;

import static comparators.Composite.serializeToByteBuffer;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.junit.Test;

import comparators.Composite;
import comparators.CompositeType;

public class CompositeTest
{

    private static final Logger logger = Logger.getLogger(CompositeTest.class
            .getName());

    static CompositeType comparer = new CompositeType();

    public static java.util.UUID getTimeUUID()
    {
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

    public static void testCompare(ByteBuffer o1, ByteBuffer o2, int expected)
    {
        int c = comparer.compare(o1, o2);
        logger.info(comparer.getString(o1)
                + (c > 0 ? " > " : c < 0 ? " < " : " = ")
                + comparer.getString(o2));
        if (expected == 0)
        {
            assertTrue(c == expected);
        } else
        {
            assertTrue((c < 0) == (expected < 0));
        }
    }

    public static void testCompare(Composite c1, Composite c2, int expected)
    {
        int c = c1.compareTo(c2);
        logger.info(c1.toString() + (c > 0 ? " > " : c < 0 ? " < " : " = ")
                + c2.toString());
        if (expected == 0)
        {
            assertTrue(c == expected);
        } else
        {
            assertTrue((c < 0) == (expected < 0));
        }
    }

    @Test
    public void testCompare()
    {

        // Create and serialize composite value

        Composite c1 = new Composite("smith", "bob", System.currentTimeMillis());
        ByteBuffer o1 = c1.serializeToByteBuffer();
        logger.info(c1.toString());

        // logger.info(comparer.getString(o1) + " is encoded as "
        // + FBUtilities.bytesToHex(o1));

        // Deserialize composite value

        Composite c2 = new Composite(o1);
        logger.info(c2.toString());

        assertTrue(c1.compareTo(c2) == 0);

        // Test comparisons

        // smith,bob,timestamp > hello,256
        testCompare(o1, serializeToByteBuffer("hello", 256), 1);

        // uuid1,256 < uuid2,256
        testCompare(serializeToByteBuffer(getTimeUUID(), 256),
                serializeToByteBuffer(getTimeUUID(), 256), -1);

        // 256 = 256
        testCompare(serializeToByteBuffer(256), serializeToByteBuffer(256), 0);

        // 256 > 256,MATCH_MINIMUM
        testCompare(serializeToByteBuffer(256),
                serializeToByteBuffer(256, Composite.MATCH_MINIMUM), 1);

        // 256,MATCH_MINIMUM < 256,0
        testCompare(serializeToByteBuffer(256, Composite.MATCH_MINIMUM),
                serializeToByteBuffer(256, 0), -1);

        // 256,10 < 256,MATCH_MAXIMUM
        testCompare(serializeToByteBuffer(256, 10),
                serializeToByteBuffer(256, Composite.MATCH_MAXIMUM), -1);

        // alpha < beta
        testCompare(serializeToByteBuffer("alpha"),
                serializeToByteBuffer("beta"), -1);

        // alpha,MATCH_MINIMUM < alpha
        testCompare(serializeToByteBuffer("alpha", Composite.MATCH_MINIMUM),
                serializeToByteBuffer("alpha"), -1);

        // alpha < alpha,MATCH_MAXIMUM
        testCompare(serializeToByteBuffer("alpha"),
                serializeToByteBuffer("alpha", Composite.MATCH_MAXIMUM), -1);

    }

}
