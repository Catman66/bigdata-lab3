import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class MyCounter {
    public static void main(String[] args) {
        try {
            countAndWrite("output/counts", "output/final");
        } catch (Exception e) {
            e.printStackTrace();// TODO: handle exception
        }
    }

    
}
