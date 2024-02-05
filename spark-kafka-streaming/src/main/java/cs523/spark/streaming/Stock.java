package cs523.spark.streaming;


public class Stock {
    private String date;
    private float open;
    private float high;
    private float low;
    private float close;
    private float volumn;

    public Stock() {}
    public Stock(String date, float open, float high, float low, float close, float volumn) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volumn = volumn;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public float getOpen() {
        return open;
    }

    public void setOpen(float open) {
        this.open = open;
    }

    public float getHigh() {
        return high;
    }

    public void setHigh(float high) {
        this.high = high;
    }

    public float getLow() {
        return low;
    }

    public void setLow(float low) {
        this.low = low;
    }

    public float getClose() {
        return close;
    }

    public void setClose(float close) {
        this.close = close;
    }

    public float getVolumn() {
        return volumn;
    }

    public void setVolume(float volumn) {
        this.volumn = volumn;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "date='" + date + '\'' +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", volumn=" + volumn +
                '}';
    }
}

