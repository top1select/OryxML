package com.cloudera.oryx.common.lang;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;


public class JVMUtils {

    private static final Logger log = LoggerFactory.getLogger(JVMUtils.class);

    private static final OryxShutdownHook SHUTDOWN_HOOK = new OryxShutdownHook();

    private JVMUtils() {}


    public static void closeAtShutdown(Closeable closeable) {
        if (SHUTDOWN_HOOK.addCloseable(closeable)) {
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK, "OryxShutdownHookThread"));
            } catch (IllegalStateException ise) {
                log.warn("Cant close {} at shutdown since it's in progress", closeable);
            }
        }
    }
}
