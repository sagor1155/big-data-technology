package cs523.spark.sqlclient;

import java.io.Serializable;

public class Stock implements Serializable {
	
	private String date;
    private String open;
    private String high;
    private String low;
    private String close;
    private String volumn;

    public Stock() {}
    public Stock(String date, String open, String high, String low, String close, String volumn) {
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

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public String getHigh() {
        return high;
    }

    public void setHigh(String high) {
        this.high = high;
    }

    public String getLow() {
        return low;
    }

    public void setLow(String low) {
        this.low = low;
    }

    public String getClose() {
        return close;
    }

    public void setClose(String close) {
        this.close = close;
    }

    public String getVolumn() {
        return volumn;
    }

    public void setVolume(String volumn) {
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
