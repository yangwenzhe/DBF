package util;

import entity.feature_t;
import entity.signature_t;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class relax_EMD {
    double Dist(feature_t F1, feature_t F2) {
        double dX = F1.X - F2.X, dY = F1.Y - F2.Y;//F1.Z - F2.Z;
        return Math.pow(dX * dX + dY * dY, 0.5);
    }
    double EuclideanDistance(double[] x, double[] y){
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return Math.sqrt(d);
    }
    public static double disBetweenTwoPoint(feature_t F1, feature_t F2) {
        double dX = F1.X - F2.X;
        double dY = F1.Y - F2.Y;
        return Math.sqrt((double)(dX*dX + dY*dY));
    }

    public static double getMinWeights(signature_t s1,signature_t s2){
        double totalWeights1 = 0.0;
        double totalWeights2 = 0.0;
        for (int i = 0; i < s1.n; i++){
            totalWeights1 += s1.Weights[i];
        }
        for (int j = 0; j < s2.n; j++){
            totalWeights2 += s2.Weights[j];
        }
        return Math.max(totalWeights1,totalWeights2);
    }
//    public double ICT(signature_t s1, signature_t s2) throws CloneNotSupportedException{
//        signature_t p = (signature_t) s1.clone();
//        signature_t query = (signature_t) s2.clone();
//        double total_cost = 0.0;
//        ArrayList<TreeMap< Double, Integer>> DistMatrix = new ArrayList<>();
//        double[][] distanceMatrix = new double[p.n][query.n];
//        for (int i = 0; i< p.n; i++){
//            TreeMap<Double,Integer> hm = new TreeMap<>();
//            for (int j = 0; j<query.n; j++){
//                distanceMatrix[i][j] = Dist(p.Features[i], query.Features[j]);
//                hm.put(distanceMatrix[i][j],j);
//            }
//            DistMatrix.add(hm);
//        }
//
//        for (int i = 0; i<p.n; i++){
//            TreeMap<Double,Integer> tm = DistMatrix.get(i);
//            int[] s = new int[tm.size()];
//            int sIndex = 0;
//            Iterator<Double> iterator = tm.keySet().iterator();
//            while( iterator.hasNext()){
//                Double key = iterator.next();
//                s[sIndex] = tm.get(key);
//                sIndex++;
//            }
//            int l = 0;
//            while (p.Weights[i] > 0 && l<s.length){
////                System.out.println("l = "+l);
//                double r = Math.min(p.Weights[i], query.Weights[s[l]]);
//                p.Weights[i] = p.Weights[i] - r;
//                total_cost += r*distanceMatrix[i][s[l]];
//                l++;
//            }
//        }
//        total_cost = total_cost;
//        return total_cost;
//    }
    public double ICT(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;
        ArrayList<HashMap<Integer, Double>> DistMatrix = new ArrayList<>();
        double[][] distanceMatrix = new double[p.n][query.n];
        for (int i = 0; i< p.n; i++){
            HashMap<Integer, Double> hm = new HashMap<>();
            for (int j = 0; j<query.n; j++){
                distanceMatrix[i][j] = Dist(p.Features[i], query.Features[j]);
                hm.put(j,distanceMatrix[i][j]);
            }
            DistMatrix.add(hm);
        }
        for (int i=0; i<p.n; i++){
//            System.out.println("i ==" + i);
//            System.out.println("tm.size()="+tm.size());
            double[] ithMatrix = distanceMatrix[i].clone();
            int[] s = sort(ithMatrix, false);
//            System.out.println("s.length = " + s.length);
            int l = 0;
            while (p.Weights[i] > 0.000000000000000001 && l < s.length){
//                System.out.println("l = "+l);
//                System.out.println("p.Weights["+i+"] = "+p.Weights[i]);
                double r = Math.min(p.Weights[i], query.Weights[s[l]]);
                p.Weights[i] = p.Weights[i] - r;
//                System.out.println("p.Weights[i] - r = "+p.Weights[i]);
                total_cost += r * distanceMatrix[i][s[l]]; //tm.get(s[l]);// distanceMatrix[i][s[l]];
                l++;
            }
        }
        total_cost = total_cost;
        return total_cost;
    }


    public double tighter_sort_ICT(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;

        double[] weighrs_array = new double[p.n];
        for (int i = 0; i<p.n; i++){
            weighrs_array[i] = p.Weights[i];
        }
        int[] order = sort1(weighrs_array, true);

        ArrayList<HashMap<Integer, Double>> DistMatrix = new ArrayList<>();
        double[][] distanceMatrix = new double[p.n][query.n];
        for (int i = 0; i< p.n; i++){
            HashMap<Integer, Double> hm = new HashMap<>();
            for (int j = 0; j<query.n; j++){
                distanceMatrix[i][j] = Dist(p.Features[i], query.Features[j]);
                hm.put(j,distanceMatrix[i][j]);
            }
            DistMatrix.add(hm);
        }

        for (int i : order){
            double[] ithMatrix = distanceMatrix[i].clone();
            int[] s = sort(ithMatrix, false);
            int l = 0;
            while (p.Weights[i] > 0.000000000000000001 && l<s.length){
                if (p.Weights[i] <= query.Weights[s[l]]){
                    double r = p.Weights[i]; //  r is the smaller value;
                    p.Weights[i] = 0;
                    total_cost += r * distanceMatrix[i][s[l]];
                }else{
                    double r = query.Weights[s[l]]; //  r is the smaller value;
                    p.Weights[i] = p.Weights[i] - r;
                    query.Weights[s[l]] = 0;
                    total_cost += r * distanceMatrix[i][s[l]];
                }
                l++;
            }
        }
        total_cost = total_cost;
        return total_cost;
    }
    public double tighter_ICT(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;

        ArrayList<HashMap<Integer, Double>> DistMatrix = new ArrayList<>();
        double[][] distanceMatrix = new double[p.n][query.n];
        for (int i = 0; i< p.n; i++){
            HashMap<Integer, Double> hm = new HashMap<>();
            for (int j = 0; j<query.n; j++){
                distanceMatrix[i][j] = Dist(p.Features[i], query.Features[j]);
                hm.put(j,distanceMatrix[i][j]);
            }
            DistMatrix.add(hm);
        }

        for (int i=0; i<p.n; i++){
            double[] ithMatrix = distanceMatrix[i].clone();
            int[] s = sort(ithMatrix, false);
            int l = 0;
            while (p.Weights[i] > 0.000000000000000001 && l<s.length){
                if (p.Weights[i] <= query.Weights[s[l]]){
                    double r = p.Weights[i]; //  r is the smaller value;
                    p.Weights[i] = 0;
                    total_cost += r * distanceMatrix[i][s[l]];
                }else{
                    double r = query.Weights[s[l]]; //  r is the smaller value;
                    p.Weights[i] = p.Weights[i] - r;
                    query.Weights[s[l]] = 0;
                    total_cost += r * distanceMatrix[i][s[l]];
                }
                l++;
            }
        }
        total_cost = total_cost;
        return total_cost;
    }

    public double IM_SIG_star(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;
        double distance[][] = new double[p.n][query.n];
        for(int i=0;i<p.n;i++){
            for(int j=0;j<query.n;j++){
                distance[i][j] = disBetweenTwoPoint(p.Features[i],query.Features[j]);
            }
        }
        Queue<LinkedList> minHeap = arraySort(distance, true);
//
        double mX = 0.0;
        double mY = 0.0;
        double[] sourceCap = new double[p.n];
        for (int i = 0; i < p.n; i++){
            mX += p.Weights[i];
            sourceCap[i] = p.Weights[i];
        }
        for (int i = 0; i < query.n; i++){
            mY += query.Weights[i];
        }
        double minWeight = Math.min(mX, mY);
//
        double remainingEarth = minWeight;
        while (remainingEarth > 0.000000000000000001 && !minHeap.isEmpty()){ // //0.00000000001
            LinkedList list = minHeap.poll();
            int[] index = (int[]) list.poll();
            int x = index[0];
            int y = index[1];
            double dis = (double) list.poll();
            if (sourceCap[x] > 0.000000000000000001){ //0.00000000001
                double earth;
                if (query.Weights[y] >= remainingEarth){
                    earth = Math.min(remainingEarth, sourceCap[x]);
                }else {
                    earth = Math.min(sourceCap[x], query.Weights[y]);
                }
                total_cost += dis*earth;
                sourceCap[x] -= earth;
                remainingEarth -= earth;
            }
        }
        return total_cost/minWeight;
    }

    public double upper_bound_test_ICT(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;

        double[] weighrs_array = new double[p.n];
        for (int i = 0; i<p.n; i++){
            weighrs_array[i] = p.Weights[i];
        }
        int[] order = sort1(weighrs_array, false);

        ArrayList<HashMap<Integer, Double>> DistMatrix = new ArrayList<>();
        double[][] distanceMatrix = new double[p.n][query.n];
        for (int i = 0; i< p.n; i++){
            HashMap<Integer, Double> hm = new HashMap<>();
            for (int j = 0; j<query.n; j++){
                distanceMatrix[i][j] = Dist(p.Features[i], query.Features[j]);
                hm.put(j,distanceMatrix[i][j]);
            }
            DistMatrix.add(hm);
        }

        for (int i : order){
            double[] ithMatrix = distanceMatrix[i].clone();
            int[] s = sort(ithMatrix, false);
            int l = 0;
            while (p.Weights[i] > 0.000000000000000001 && l<s.length){
                if (p.Weights[i] <= query.Weights[s[l]]){
                    double r = p.Weights[i]; //  r is the smaller value;
                    p.Weights[i] = 0;
                    query.Weights[s[l]] -= r;
                    total_cost += r * distanceMatrix[i][s[l]];
                }else{
                    double r = query.Weights[s[l]]; //  r is the smaller value;
                    p.Weights[i] -=  r;
                    query.Weights[s[l]] = 0;
                    total_cost += r * distanceMatrix[i][s[l]];
                }
                l++;
            }
        }
        total_cost = total_cost;
        return total_cost;
    }

    public double centroid_WMD(signature_t s1, signature_t s2) throws CloneNotSupportedException{ //WCD is fast to compute, it is not very tight
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();
        double total_cost = 0.0;
        double[] centroid_p = new double[2];
        double[] centroid_query = new double[2];

        for(int i = 0; i<p.n; i++){
            centroid_p[0] += p.Weights[i] * p.Features[i].X;
            centroid_p[1] += p.Weights[i] * p.Features[i].Y;
        }
        for (int j = 0; j<query.n; j++){
            centroid_query[0] += query.Weights[j] * query.Features[j].X;
            centroid_query[1] += query.Weights[j] * query.Features[j].Y;
        }
        total_cost = EuclideanDistance(centroid_p, centroid_query);
        return total_cost;
    }
    public double removeOneConstraintEMD(signature_t s1, signature_t s2) throws CloneNotSupportedException{
        signature_t p = (signature_t) s1.clone();
        signature_t query = (signature_t) s2.clone();

        double[][] distanceMatrix1 = new double[p.n][query.n];
        for (int i = 0; i< p.n; i++){
            for (int j = 0; j<query.n; j++){
                distanceMatrix1[i][j] = Dist(p.Features[i], query.Features[j]);
            }
        }

        double[][] distanceMatrix2 = new double[query.n][p.n];
        for (int i = 0; i< query.n; i++){
            for (int j = 0; j<p.n; j++){
                distanceMatrix2[i][j] = Dist(query.Features[i], p.Features[j]);
            }
        }

        double total_cost1 = 0.0;
        for (int i=0; i<p.n; i++){
            double[] ithMatrix1 = distanceMatrix1[i].clone();
            int[] s = sort(ithMatrix1, false);
            double r1 = p.Weights[i];
            total_cost1 += r1 * distanceMatrix1[i][s[0]];
        }
        total_cost1 = total_cost1;

        double total_cost2 = 0.0;
        for (int j=0; j<query.n; j++){
            double[] ithMatrix2 = distanceMatrix2[j].clone();
            int[] s = sort(ithMatrix2, false);
            double r2 = query.Weights[j];
            total_cost2 += r2 * distanceMatrix2[j][s[0]];
        }
        total_cost2 = total_cost2;

        return Math.min(total_cost1, total_cost2);

    }

    public Queue<LinkedList> arraySort(double[][] array, boolean desc){
        double[][] arr = array.clone();
        double[] a = new double[arr.length*arr[0].length];
        int count = 0;
        for (int i = 0; i <arr.length; i++){
            for (int j = 0; j<arr[0].length; j++){
                a[count] = arr[i][j];
                count++;
            }
        }
        int[] index = sort(a,desc);//index是升序之后的索引，a也会从低到高排序
        Queue<LinkedList> queue = new LinkedList<>();

        for (int i = index.length-1; i>=0; i--){
            int[] xy = new int[2];
            int x = index[i]/arr[0].length;
            int y = index[i]%arr[0].length;
            xy[0] = x;
            xy[1] = y;
            LinkedList list = new LinkedList();
            list.add(xy);
            list.add(a[i]);
           ((LinkedList<LinkedList>) queue).add(list);
        }
        return queue;
    }
    // sort1 and sort is different
    //sort1 返回排序之后的索引，但是原数组不变
    public int[] sort1(double[] array, boolean desc){
        double[] arr = array.clone();
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
    //sort返回排序之后的索引和排序后的数组
    public int[] sort(double[] arr, boolean desc){
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
    public int[] sort(double[] arr, int[] Index, boolean desc){
        double temp;
        int index;

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