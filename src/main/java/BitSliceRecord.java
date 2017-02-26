public class BitSliceRecord {
    private int value;
    private String bitmap;
    private long count;

    public int getValue() {
        return value;
    }

    public String getBitmap() {
        return bitmap;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setBitmap(String bitmap) {
        this.bitmap = bitmap;
    }

    public double getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
