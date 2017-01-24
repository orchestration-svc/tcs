package net.messaging.clusterbox.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

public class CommandRunner implements Runnable {

    private final Object targetInstance;
    private final Class<?> targetClass;

    public static class MyScanner {
        private BufferedReader br;
        private StringTokenizer st;

        public MyScanner(InputStream stream) {
            br = new BufferedReader(new InputStreamReader(stream));
        }

        public boolean hasMore() {
            if (st != null && st.hasMoreTokens()) {
                return true;
            }
            return false;
        }

        public String getNextLine() {
            if (st == null || !st.hasMoreTokens()) {
                try {
                    String data = br.readLine();
                    if (StringUtils.isEmpty(data)) {
                        return null;
                    }
                    st = new StringTokenizer(data);
                } catch (IOException e) {

                }
            }
            return st.nextToken("\n");
        }
    };

    /*
     * Returns index 0 is always command and remaining are arguments
     */
    private List<String> getCommandAndParams(String commandLine) {
        List<String> commandParams = null;
        if (StringUtils.isNotEmpty(commandLine)) {
            commandParams = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(commandLine);
            commandParams.add(st.nextToken());
            while (st.hasMoreTokens()) {
                commandParams.add(st.nextToken());
            }
        }
        return commandParams;
    }

    public Method getMethod(String command) {
        Method[] methods = targetClass.getMethods();
        for (Method method : methods) {
            CliRunner runner = method.getAnnotation(CliRunner.class);
            if (runner != null) {
                String annotatedCommand = runner.command();
                if (StringUtils.equalsIgnoreCase(annotatedCommand, command)) {
                    return method;
                }
            }
        }
        return null;
    }

    public List<String> getAllAnnotatedCommand() {
        List<String> commandList = new ArrayList<String>();
        Method[] methods = targetClass.getMethods();
        for (Method method : methods) {
            CliRunner runner = method.getAnnotation(CliRunner.class);
            if (runner != null) {
                System.out.println(runner.command());
            }
        }
        return commandList;
    }

    public void executeMethod(Method method, Object[] arguments) {
        if (method != null) {
            try {
                method.invoke(targetInstance, arguments);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                System.out.println("Method Invocation failed " + method);
                e.printStackTrace();
            }
        } else {
            System.out.println("UnSupported command");
        }
    }

    @Override
    public void run() {
        MyScanner scanner = new MyScanner(System.in);
        while (true) {
            System.out.print("Enter Command : ");
            try {
                String commandLine = scanner.getNextLine();
                if (StringUtils.isEmpty(commandLine)) {
                    continue;
                }
                List<String> commandAndParams = getCommandAndParams(commandLine);
                if (CollectionUtils.isNotEmpty(commandAndParams)) {
                    if (StringUtils.equalsIgnoreCase("list", commandAndParams.get(0))) {
                        getAllAnnotatedCommand();
                    } else {
                        Method method = getMethod(commandAndParams.get(0));
                        executeMethod(method, commandAndParams.subList(1, commandAndParams.size()).toArray());
                    }
                }
            } catch (Exception e) {
                System.out.println("Error " + e);
                e.printStackTrace();
            }
        }
    }

    public CommandRunner(Object targetInstance) {
        this.targetInstance = targetInstance;
        this.targetClass = targetInstance.getClass();
        Thread executor = new Thread(this);
        executor.setName("CommandRunning-Thread");
        executor.start();
        try {
            executor.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
