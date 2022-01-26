package util;

import entity.feature_t;
import entity.signature_t;
import org.checkerframework.checker.units.qual.min;

import java.util.ArrayList;
import java.util.HashMap;

public class pooling {
    static double ub_move;

    public double getUb() {
        return ub_move;
    }

    public static signature_t poolingOperature1(signature_t s1, int t) throws CloneNotSupportedException{
        int T = t;
        Object obj = s1.clone();
        ub_move = 0.0;
        signature_t s2 = (signature_t) obj;
        signature_t s3 = new signature_t();
        HashMap<Integer, double[]> hm = new HashMap<>();
        double[] d= new double[s2.n];
//        double totalWeights = 0.0;
        if (s2.n == 1){
            s3 = (signature_t) s2.clone();
        }else{
        for (int i = 0; i<s2.n; i++){
//            totalWeights += s2.Weights[i];
            d[i] = s2.Weights[i];
            double[] xyCorrd = new double[2];
            xyCorrd[0] = s2.Features[i].X;
            xyCorrd[1] = s2.Features[i].Y;
            hm.put(i,xyCorrd);
        }
        int[] index = sort(d,false);
        for (int i=0; i<index.length-1; i++){
            int minIndex = i;
            int secIndex = i+1;
            double distance = EuclideanDistance(hm.get(minIndex),hm.get(secIndex));
            double min = d[i];
            ub_move += distance * min;
            s2.Weights[secIndex] = s2.Weights[secIndex] + s2.Weights[minIndex];
            d[secIndex] = d[secIndex] + d[minIndex];
            s2.Weights[minIndex] = 0;
            d[minIndex] = 0;
        }

        ub_move = ub_move;
        s3 = deleteZeroBin(s2);
        }
        return s3;
    }

    public signature_t poolingOperature (signature_t s1, int t) throws CloneNotSupportedException{
        Object obj = s1.clone();
        ub_move = 0.0;
        signature_t s2 = (signature_t) obj;
        double totalWeights = 0.0;
        for (int i = 0; i<s2.n; i++){
            totalWeights += s2.Weights[i];
        }
        signature_t s3 = new signature_t();
        int minIndex = 100000000;
        int secIndex = 100000000;
        while(numberOfNoneZero(s2) > t){
//            System.out.println("numberOfNoneZero(s2)"+numberOfNoneZero(s2));
            double min = 100000000;
            double sec = 100000000;

            for (int i =0; i<s2.n; i++){
                if (min > s2.Weights[i] && s2.Weights[i] > 0){
                    min = s2.Weights[i];
                    minIndex = i;
                }
            }
            for (int j =0; j<s2.n; j++){
                if (sec > s2.Weights[j] && s2.Weights[j] > 0 && j!= minIndex){
                    sec = s2.Weights[j];
                    secIndex = j;
                }
            }

            double distance = disBetweenTwoPoint(s2.Features[minIndex],s2.Features[secIndex]);
            ub_move += distance * min;
            s2.Weights[secIndex] = s2.Weights[secIndex] + s2.Weights[minIndex];
            s2.Weights[minIndex] = 0;

        }
        ub_move = ub_move/totalWeights;
        s3 = deleteZeroBin(s2);
        return s3;
    }
    public static signature_t deleteZeroBin(signature_t s1){
        signature_t s2;
        ArrayList<Double> weights = new ArrayList<>();
        ArrayList<feature_t> features = new ArrayList<>();
        for(int i = 0; i<s1.n; i++){
            if (s1.Weights[i] >0){
                weights.add(s1.Weights[i]);
                features.add(s1.Features[i]);
            }
        }
        feature_t[] features2 = new feature_t[weights.size()];
        double[] weights2 = new double[weights.size()];
        for (int i = 0; i<weights.size(); i++){
            weights2[i] = weights.get(i);
        }
        s2 = new signature_t((int)weights.size(),features.toArray(features2),weights2);

        return s2;
    }
    public static double disBetweenTwoPoint(feature_t F1, feature_t F2) {
        double dX = F1.X - F2.X;
        double dY = F1.Y - F2.Y;
        return Math.sqrt((double)(dX*dX + dY*dY));
    }

    public static double EuclideanDistance(double[] F1, double[] F2){
        double dX = F1[0] - F2[0];
        double dY = F1[1] - F2[1];
        return Math.sqrt((double)(dX*dX + dY*dY));
    }

    public static int numberOfNoneZero(signature_t s1){
        int number = 0;
        for (int i = 0; i<s1.n; i++){
            if (s1.Weights[i] > 0 ){
                number++;
            }
        }
        return number;
    }

    public static int[] sort(double[] arr, boolean desc){
        double temp;
        int index;
        int k = arr.length;
        int[] Index = new int[k];
        for (int i = 0; i < k; i++) {
            Index[i] = i;
        }

        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (desc) {
                    if (arr[j] < arr[j + 1]) {
                        temp = arr[j];
                        arr[j] = arr[j + 1];
                        arr[j + 1] = temp;

                        index = Index[j];
                        Index[j] = Index[j + 1];
                        Index[j + 1] = index;
                    }
                } else {
                    if (arr[j] > arr[j + 1]) {
                        temp = arr[j];
                        arr[j] = arr[j + 1];
                        arr[j + 1] = temp;

                        index = Index[j];
                        Index[j] = Index[j + 1];
                        Index[j + 1] = index;
                    }
                }
            }
        }
        return Index;
    }
}
