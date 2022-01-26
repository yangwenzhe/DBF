package entity;


public class signature_t implements Cloneable {
    public int n;
    public feature_t[]Features;
    public double []Weights;
    public signature_t() {

    }
    public signature_t(int n, feature_t[] features, double[] weights) {
        this.n = n;
        Features = features;
        Weights = weights;
    }
    @Override
    public Object clone() throws CloneNotSupportedException {
        int i;
        signature_t o = (signature_t) super.clone();
        o.Features = (feature_t[]) o.Features.clone();

        for (i = 0; i < n; i++)
            o.Features[i] = (feature_t) o.Features[i].clone();
        o.Weights = (double[]) o.Weights.clone();

        return o;
    }
}
