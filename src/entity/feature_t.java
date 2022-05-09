package entity;

public class feature_t implements Cloneable {
    public double X;
    public double Y;
    public double Z;
    public feature_t(double x, double y) {
        this.X = x;
        this.Y = y;
    }
    public feature_t(double x, double y, double z) {
        this.X = x;
        this.Y = y;
        this.Z = z;
    }
    @Override
    public Object clone() throws CloneNotSupportedException {
        feature_t o = (feature_t) super.clone();

        return o;
    }

    public double groundDist(feature_t f) {
        feature_t f2d = (feature_t) f;
        double deltaX = X - f2d.X;
        double deltaY = Y - f2d.Y;
        return Math.sqrt((deltaX * deltaX) + (deltaY * deltaY));
    }
}
