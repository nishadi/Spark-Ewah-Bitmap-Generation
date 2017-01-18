import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.junit.Test;

/**
 * Created by nishadi on 1/5/17.
 */
public class ProjectionTest {

    @Test
    public void swaptest() {
        EWAHCompressedBitmap bm1 = new EWAHCompressedBitmap();
        bm1.set(32);
        bm1.set(68);
        bm1.set(145);
        String abc = bm1.toRLWString();
        String[] words = abc.split(",");
        Long[] array = new Long[words.length];
        for (int i = 0; i < words.length; i++) {
            array[i] = Long.parseLong(words[i]);
        }
        String yg = "fdf";
    }

}
