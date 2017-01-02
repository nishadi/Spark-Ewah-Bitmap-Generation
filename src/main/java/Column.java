import java.util.HashMap;
import java.util.Map;

import com.googlecode.javaewah.EWAHCompressedBitmap;


public class Column {

    HashMap<Integer, EWAHCompressedBitmap> bitMaps_int;
    EWAHCompressedBitmap ewahCompressedBitmap;
    HashMap<String, EWAHCompressedBitmap> bitMaps;
    String bitmapString;

    public Column() {
        bitMaps_int = new HashMap<Integer, EWAHCompressedBitmap>(16, 0.9f);
        ewahCompressedBitmap = new EWAHCompressedBitmap();
        bitMaps = new HashMap<String, EWAHCompressedBitmap>();
        bitmapString = " ";
    }

    public void setValue(String key, int position) {

        // If the key already exists, get the corresponding bitmap and set the value
        if (bitMaps.containsKey(key)) {
            bitMaps.get(key).set(position);
        }
        // Else Create a new bitmap and add it
        else {
            EWAHCompressedBitmap ewahCompressedBitmap = new EWAHCompressedBitmap();
            ewahCompressedBitmap.set(position);
            bitMaps.put(key, ewahCompressedBitmap);
        }
    }

    public void setValue(int key, int position) {
        // If the key already exists, get the corresponding bitmap and set the value
        if (bitMaps_int.containsKey(key)) {
            bitMaps_int.get(key).set(position);
            System.out.println("size : " + bitMaps_int.size());
        }
        // Else Create a new bitmap and add it
        else {
            ewahCompressedBitmap = new EWAHCompressedBitmap();
            ewahCompressedBitmap.set(position);
            bitMaps_int.put(key, ewahCompressedBitmap);
            System.out.println("size : " + bitMaps_int.size());
        }
    }

    public String getBitMaps() {
        for (Map.Entry<String, EWAHCompressedBitmap> entry : bitMaps.entrySet()) {
            String key = entry.getKey();
            EWAHCompressedBitmap value = entry.getValue();
            bitmapString += key + " : " + value.toString() + "\n";
        }
        return bitmapString;
    }
}
