package entity;

public class relaxIndexNode {
    public int resultId;
    public double lb;
    public double ub;
    public relaxIndexNode(int id){
        this.resultId = id;
    }
    public relaxIndexNode(int id, double lb){
        this.resultId = id;
        this.lb = lb;
    }
    public relaxIndexNode(int id, double lb, double ub){
        this.resultId = id;
        this.lb = lb;
        this.ub = ub;
    }
    public int getResultId(){
        return this.resultId;
    }

    public void setLb(double lb){
        this.lb = lb;
    }
    public double getLb(){
        return this.lb;
    }

    public void setUb(double ub){
        this.ub = ub;
    }
    public double getUb(){
        return this.ub;
    }
}
