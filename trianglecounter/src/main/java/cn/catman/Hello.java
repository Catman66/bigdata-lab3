import java.io.File;

public class Hello {
    public static void main(String[] args) {
        System.out.println("hello world\n");
        System.out.println(args[0]);

        File f = new File("tmpdir/tmp.txt");
        f.createNewFile();  
        
    }
}
