package smallsql.junit;

/**
 * Adapt Junit5 to Junit 3
 */
public class JunitTestExtended extends org.junit.jupiter.api.Assertions {

    public static void assertNotSame(String msg, Object u, Object a) {
        org.junit.jupiter.api.Assertions.assertNotSame(u, a, msg);
    }

    public static void assertTrue(String msg, boolean t) {
        org.junit.jupiter.api.Assertions.assertTrue(t, msg);
    }

    public static void assertFalse(String msg, boolean t) {
        org.junit.jupiter.api.Assertions.assertFalse(t, msg);
    }

    public static void assertEquals(String msg, Object e, Object a) {
        org.junit.jupiter.api.Assertions.assertEquals(e, a, msg);
    }

    public static void assertEquals(String msg, double expected, double actual, double delta) {
        org.junit.jupiter.api.Assertions.assertTrue(Math.abs(expected - actual) <= delta, msg);
    }

    public static void assertNotNull(String msg, Object o) {
        org.junit.jupiter.api.Assertions.assertNotNull(o, msg);
    }

    public static void assertNull(String msg, Object o) {
        org.junit.jupiter.api.Assertions.assertNull(o, msg);
    }

}
