package entity;

import au.edu.rmit.trajectory.clustering.kmeans.indexNode;

public class indexNodeExpand {
    public indexNode in;
    public double lb;
    public double ub;
    public indexNodeExpand(indexNode index){
        this.in = index;
    }
    public indexNodeExpand(indexNode index, double lb){
        this.in = index;
        this.lb = lb;
    }
    public indexNodeExpand(indexNode index, double lb, double ub){
        this.in = index;
        this.lb = lb;
        this.ub = ub;
    }
    public indexNode getin(){
        return this.in;
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
