package ch.psi.daq.imageapi;

public class TestInfo {

    static void test() {
        ClassLoader classLoader = Main.class.getClassLoader();
        try {
            Class<?> cl1 = classLoader.loadClass("ch.psi.imageapi3.Info");
            cl1.getMethod("setup").invoke(null);
        }
        catch (ClassNotFoundException e) {
        }
        catch (Throwable e) {
        }
    }

}
