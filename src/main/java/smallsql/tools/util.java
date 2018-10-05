package smallsql.tools;

/**
 * Some utilities
 */
public class util {

    /**
     * Generate an exception for unsupported operation
     */
    public static UnsupportedOperationException generateUnsupportedOperation() {
        String method = "";
        var err = new Exception().getStackTrace();
        if (err.length > 1) {
            method = err[1].getClassName() + ": " + err[1].getMethodName();
        }
        return new UnsupportedOperationException(String.format("<%s> not yet implemented.", method));
    }

}
