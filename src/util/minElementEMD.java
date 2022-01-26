package util;


import entity.feature_t;
import entity.flow_t;
import entity.signature_t;

import java.util.ArrayList;
import java.util.List;

public class minElementEMD {
    public static double lower_emd(signature_t s1_origin, signature_t s2_origin) throws CloneNotSupportedException {
        int size_1,size_2;
        double distance[][];
        boolean isUsed[][];
        double res = 0;
        double yield = 0;
        double sale = 0;
        double trans = 0;

        List<flow_t> list = new ArrayList<flow_t>();
        signature_t s1 = (signature_t)s1_origin.clone();
        signature_t s2 = (signature_t)s2_origin.clone();
        size_1 = s1.n;
        size_2 = s2.n;
        distance = new double[size_1][size_2];
        isUsed= new boolean[size_1][size_2];
        for(int i=0;i<size_1;i++){
            for(int j=0;j<size_2;j++){
                distance[i][j] = disBetweenTwoPoint(s1.Features[i],s2.Features[j]);
            }
        }
        //calculateTrans(s1,s2);//???????
        for(double w:s1.Weights)
            yield += w;

        for(double w:s2.Weights)
            sale += w;

        trans = Math.min(yield,sale);
//        System.out.println("yield = "+yield);
//        System.out.println("sale = "+sale);

        double test=0.0;
        while(trans>0.000000000001){
            int []path = getMinDis(size_1,size_2,distance,isUsed);//
            int i = path[0];int j = path[1];
            double amount = 0;//
            if(s1.Weights[i]!=0 && s2.Weights[j]!=0){
                if(s1.Weights[i]>s2.Weights[j]){
                    amount = s2.Weights[j];

                    s1.Weights[i]-=amount;
                    s2.Weights[j]=0;
                }else {// s1.Weights[i] <= s2.Weights[j]
                    amount = s1.Weights[i];
                    s2.Weights[j]-=amount;
                    s1.Weights[i]=0;
                }
                res+=amount*distance[i][j];
                list.add(new flow_t(i,j,amount));
                trans-=amount;
                test += amount;
//                System.out.println("trans:"+trans);
            }

            isUsed[i][j]=true;
        }

        return res;///Math.min(yield,sale);



    }

    public static double emd(signature_t s1_origin, signature_t s2_origin) throws CloneNotSupportedException{
        int size_1,size_2;
        double distance[][];
        boolean isUsed[][];
        double res = 0;
        double yield = 0;
        double sale = 0;
        double trans = 0;

        List<flow_t> list = new ArrayList<flow_t>();
        signature_t s1 = (signature_t)s1_origin.clone();
        signature_t s2 = (signature_t)s2_origin.clone();
        size_1 = s1.n;
        size_2 = s2.n;
        distance = new double[size_1][size_2];
        isUsed= new boolean[size_1][size_2];
        for(int i=0;i<size_1;i++){
            for(int j=0;j<size_2;j++){
                distance[i][j] = disBetweenTwoPoint(s1.Features[i],s2.Features[j]);
            }
        }
        for(double w:s1.Weights)
            yield += w;

        for(double w:s2.Weights)
            sale += w;

        trans = Math.min(yield,sale);
//        System.out.println("yield = "+yield);
//        System.out.println("sale = "+sale);

        double test=0.0;
        while(trans>0.000000000001){
            int []path = getMinDis(size_1,size_2,distance,isUsed);//
            int i = path[0];int j = path[1];
            double amount = 0;//
            if(s1.Weights[i]!=0 && s2.Weights[j]!=0){
                if(s1.Weights[i]>s2.Weights[j]){
                    amount = s2.Weights[j];

                    s1.Weights[i]-=amount;
                    s2.Weights[j]=0;
                }else {// s1.Weights[i] <= s2.Weights[j]
                    amount = s1.Weights[i];
                    s2.Weights[j]-=amount;
                    s1.Weights[i]=0;
                }
                res+=amount*distance[i][j];
                list.add(new flow_t(i,j,amount));
                trans-=amount;
                test += amount;
//                System.out.println("trans:"+trans);//??????????
            }

            isUsed[i][j]=true;//??????????
        }

        return res;///Math.min(yield,sale);
    }

    public static int[] getMinDis(int size_1,int size_2, double[][] distance, boolean[][] isUsed){
        double min = Integer.MAX_VALUE;
        int []path = new int[2];
        for(int i=0;i<size_1;i++){
            for(int j=0;j<size_2;j++){
                if(distance[i][j]<min && !isUsed[i][j]){
                    min = distance[i][j];
                    path[0]=i;path[1]=j;
                }
            }
        }
        return path;
    }

    /* ??????????? */
    public static double disBetweenTwoPoint(feature_t F1, feature_t F2) {
        double dX = F1.X - F2.X;
        double dY = F1.Y - F2.Y;
        return Math.sqrt((double)(dX*dX + dY*dY));
    }
}
