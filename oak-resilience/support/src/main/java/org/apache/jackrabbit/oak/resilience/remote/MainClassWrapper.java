package org.apache.jackrabbit.oak.resilience.remote;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MainClassWrapper {

    public static void main(String[] args) throws Throwable {
        Class<?> clazz = Class.forName(args[0]);
        String[] mainArgs = new String[args.length - 1];
        for (int i = 0; i < mainArgs.length; i++) {
            mainArgs[i] = args[i + 1];
        }

        Method method = clazz.getMethod("main", args.getClass());
        try {
            method.invoke(null, (Object) mainArgs);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            RemoteMessageProducer.close();
        }
    }

}