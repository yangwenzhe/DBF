package entity;

public class feature_t implements Cloneable {
    public double X;
    public double Y;
    public feature_t(double x, double y) {
        this.X = x;
        this.Y = y;
    }
    @Override
    public Object clone() throws CloneNotSupportedException {
        feature_t o = (feature_t) super.clone();

        return o;
    }

}
