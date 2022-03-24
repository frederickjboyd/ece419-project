package shared;

import org.apache.log4j.Logger;

public class DebugHelper {
    /**
     * Helper function to log a function's entry point.
     * 
     * @param logger Log4j logger
     */
    public static void logFuncEnter(Logger logger) {
        String funcName = Thread.currentThread().getStackTrace()[2].getMethodName();
        logger.trace(String.format("%s enter", funcName));
    }

    /**
     * Helper function to log a function's exit point.
     * 
     * @param logger Log4j logger
     */
    public static void logFuncExit(Logger logger) {
        String funcName = Thread.currentThread().getStackTrace()[2].getMethodName();
        logger.trace(String.format("%s exit", funcName));
    }
}
