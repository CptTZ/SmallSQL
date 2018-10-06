package smallsql.misc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import smallsql.tools.util;

class utilTest {

    @Test
    void testUnsupported() {
        Exception e = util.generateUnsupportedOperation();
        String msg = e.getLocalizedMessage();
        Assertions.assertTrue(msg.startsWith("<smallsql.misc.utilTest: testUnsupported>"));
    }

}
