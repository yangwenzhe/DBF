package ballTree;

import au.edu.rmit.trajectory.clustering.kmeans.indexNode;
import entity.feature_t;
import entity.indexNodeExpand;
import entity.relaxIndexNode;
import entity.signature_t;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import util.emd_class;
import util.minElementEMD;
import util.relax_EMD;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ball_tree_index {
    //global variable
    public static int dimension = 2; //dimension
    public static int totalNumberOfLine = 30; //  total row number of query dataset in test.txt
    public static int topk = 5;
    public static double[] query; // histogram of query data
    public static double queryRadius; //radius of query data
    public static double[][] iterMatrix; // all histogram of data lake
    public static double[] ubMove; // each data's ubMove in data lake
    public static int[] histogram_name;// histogram_name[resultID-1] indicate the dataset ID
    public static signature_t querySignature;
    //Integer is row of iterMatrix, ArrayList is corrdinate of each bin in histogram
    public static HashMap<Integer, ArrayList<double[]>> allHistogram = new HashMap<>();//store all histogram in database except query
    public static int queryDataRowNumber = 1; //  number of row querydata in histogram.txt, instead of dataset ID


    //input row of iterMatrix, output the signature;
    public static signature_t getSignature(int id) {
        ArrayList<double[]> a = allHistogram.get(id);
        int n = a.size();
        feature_t[] features = new feature_t[n];
        double[] weights = new double[n];
        for (int i = 0; i < n; i++) {
            features[i] = new feature_t(a.get(i)[0], a.get(i)[1]);
            weights[i] = a.get(i)[2];
        }
        signature_t dataSignature = new signature_t(n, features, weights);
        return dataSignature;
    }


    public static void getAllData() throws IOException {
        String histogram = "D:\\GitHub\\Argov\\data\\histogram.txt";
//        HashMap<Integer, ArrayList<double[]>> allHistogram = new HashMap<>();
        FileInputStream fis = new FileInputStream(histogram);
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(isr);
        String str;
        String[] buff;
        str = br.readLine();
        int numberOfLine = 1;// the first row in histogrm.txt
        while (str != null) {
            ArrayList<double[]> allPoints = new ArrayList<>();
            buff = str.split(";");
            for (int i = 0; i < buff.length; i++) {
                double[] d = new double[3];//3 is x, y and weights
                String[] point = buff[i].split(",");
                for (int j = 0; j < point.length; j++) {
                    d[j] = Double.parseDouble(point[j]);
                }
                allPoints.add(d);
            }

            if (numberOfLine == queryDataRowNumber) {
                int n = allPoints.size();
                feature_t[] features = new feature_t[n];
                double[] weights = new double[n];
                for (int i = 0; i < n; i++) {
                    features[i] = new feature_t(allPoints.get(i)[0], allPoints.get(i)[1]);
                    weights[i] = allPoints.get(i)[2];
                }
                querySignature = new signature_t(n, features, weights);
                numberOfLine++;
            } else {//numberOfLine = 2,3,4.... corresponding to resultID
                if (numberOfLine < queryDataRowNumber) {
                    allHistogram.put(numberOfLine - 1, allPoints);
                } else {
                    allHistogram.put(numberOfLine - 2, allPoints);
                }
//                    System.out.println(numberOfLine - 2);
                str = br.readLine();
                numberOfLine++;
            }
        }

    }


    public static void sampleData() throws IOException {
        List<double[]> l = new ArrayList<double[]>();
        DoubleArrayList ub = new DoubleArrayList();
        ArrayList<Integer> his = new ArrayList();
        String fileName = "D:\\GitHub\\Argov\\data\\test.txt";
        FileInputStream fis = new FileInputStream(fileName);
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(isr);
        String str;
        String[] buff;
        str = br.readLine();
        int numberOfLine = 1;
        while (str != null && numberOfLine <= totalNumberOfLine) {
            if (numberOfLine == queryDataRowNumber) {
                query = new double[dimension];
                buff = str.split(",");
                query[0] = Double.parseDouble(buff[1]);
                query[1] = Double.parseDouble(buff[2]);
                queryRadius = Double.parseDouble(buff[3]);
                str = br.readLine();
                numberOfLine++;
            } else {
                double[] corrd = new double[dimension];
//            System.out.println(str);
                buff = str.split(",");
                corrd[0] = Double.parseDouble(buff[1]);
                corrd[1] = Double.parseDouble(buff[2]);
//            System.out.println("corrd = "+String.valueOf(corrd[0])+"  ,  "+String.valueOf(corrd[1]));
                l.add(corrd);
                his.add(Integer.parseInt(buff[0]));
                ub.add(Double.parseDouble(buff[3]));
//                System.out.println(Integer.parseInt(buff[0]));
                str = br.readLine();
                numberOfLine++;
            }
        }
        System.out.println("numberOfLine = " +numberOfLine);

        int countOfRow = numberOfLine - 2;// the first -1 is the array starting at 0, the second -1 is subtract the query data;
        iterMatrix = new double[countOfRow][dimension];
        ubMove = new double[countOfRow];
        histogram_name = new int[countOfRow];
        ubMove = ub.toDoubleArray();
        for (int i = 0; i < countOfRow; i++) {
            iterMatrix[i][0] = l.get(i)[0];
            iterMatrix[i][1] = l.get(i)[1];
            histogram_name[i] = his.get(i);
        }
        //       System.out.println("l.size()===="+l.size());
    }

    public static indexNode createBallTree() throws IOException {
        long startTime = System.currentTimeMillis();
        int leafThreshold = 5;
        int maxDepth = 20; // 2^20 = 104 8576
        sampleData();
        indexNode in = ball_tree.create(ubMove, iterMatrix, leafThreshold, maxDepth, dimension);
        long endTime = System.currentTimeMillis(); // current time
        System.out.println("ballTree.getTotalCoveredPoints()==" + in.getTotalCoveredPoints());
        System.out.println("create balltree time==== " + (endTime - startTime) + "ms");
        return in;
    }
    public static ArrayList<Integer> RangeSearch(indexNode root) {
        ArrayList<Integer> resultID = new ArrayList<>();
        if (root.isLeaf()) {
            System.out.println("distance(root.getPivot(), query)======"+distance(root.getPivot(), query));
            System.out.println("root.getEMDRadius()====="+root.getEMDRadius());
            double LowerBound = distance(root.getPivot(), query) - queryRadius - root.getEMDRadius();
            if (LowerBound < 0) { //interct with query circle
                resultID.addAll(root.getpointIdList());
            }
        } else {
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aIndexNode : listnode) {
                double LowerBound = distance(aIndexNode.getPivot(), query) - queryRadius - aIndexNode.getEMDRadius();
                if (LowerBound < 0) {
                    resultID.addAll(RangeSearch(aIndexNode));
                }
            }
        }
        return resultID;
    }

    public static double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return d;
    }

    public static double distance(double[] x, double[] y) {
        return Math.sqrt(distance2(x, y));
    }

    //global variant
    public static PriorityQueue<indexNodeExpand> tm = new PriorityQueue<>(new ComparatorByIndexNodeExpand());
    public static double min_dis = Double.MAX_VALUE;
    public static void BestFirst(indexNode root, int k){
        if (root.isLeaf()){//leaf node
            double LowerBound = distance(root.getPivot(),query) - root.getEMDRadius();
            indexNodeExpand in = new indexNodeExpand(root,LowerBound);
            if (tm.size()<k){
                tm.add(in);
                min_dis = tm.peek().getLb();
            }
            else if (LowerBound < min_dis){
                tm.poll();
                tm.add(in);
                min_dis = tm.peek().getLb();
            }
        }
        else {//internal node
            Set<indexNode> listnode = root.getNodelist();
            ArrayList<indexNode> indexList= new ArrayList<>();
            for (indexNode aIndexNode: listnode) {
                indexList.add(aIndexNode);
            }

            indexNode leftChildren = indexList.get(0);
            double ld = distance(leftChildren.getPivot(), query) - leftChildren.getEMDRadius();
            indexNode rightChildren = indexList.get(1);
            double rd = distance(rightChildren.getPivot(), query) - rightChildren.getEMDRadius();

            if (ld > min_dis && rd > min_dis){  System.out.println("ld,rd > min_dis"); }//no sense looking here
            else if(ld <= rd){
                System.out.println("ld=="+ld);
                System.out.println("min_dis=="+min_dis);
                System.out.println( ld<min_dis );
                BestFirst(leftChildren, k);
                if (rd < min_dis){
                    BestFirst(rightChildren, k);
                }
            }else {
                BestFirst(rightChildren,k);
                if (ld<min_dis){
                    BestFirst(leftChildren,k);
                }
            }

        }
    }
    public static ArrayList<Integer> getBestFirstResultID(indexNode root, int k){
        ArrayList<Integer> resultID = new ArrayList<>();
        BestFirst(root, k);
        while (!tm.isEmpty()){
            indexNodeExpand in = tm.poll();
            resultID.addAll(in.getin().getpointIdList());
        }

        return resultID;
    }

    public static PriorityQueue<indexNodeExpand> PQ_Branch = new PriorityQueue<>(new ComparatorByIndexNodeExpand());
    public static double LB_Branch = 1000000000;
    public static double UB_Branch = 1000000000;
    public static void BranchAndBound(indexNode root){
        if (root.isLeaf()){//leaf node
//            System.out.println("distance(root.getPivot(), query)======"+distance(root.getPivot(), query));
//            System.out.println("root.getEMDRadius()====="+root.getEMDRadius());
            double LowerBound = distance(root.getPivot(), query) - root.getEMDRadius();
            double UpperBound = distance(root.getPivot(),query) + root.getEMDRadius();
//            System.out.print(LowerBound < 0);

            indexNodeExpand in = new indexNodeExpand(root,LowerBound,UpperBound);
            if (PQ_Branch.isEmpty()){
                PQ_Branch.add(in);
            }else if (in.getLb()>UB_Branch){
                //Pruning
            }else if(in.getUb() < LB_Branch){
//                PQ_Branch.poll();
//                PQ_Branch.add(in);
                while(in.getUb()<LB_Branch){
                    PQ_Branch.poll();
                    if (!PQ_Branch.isEmpty()){
                        LB_Branch = PQ_Branch.peek().lb;
                        UB_Branch = PQ_Branch.peek().ub;
                    } else{
                        break;
                    }
                }
                PQ_Branch.add(in);
            }
            else{
                PQ_Branch.add(in);
            }
            LB_Branch = PQ_Branch.peek().lb;
            UB_Branch = PQ_Branch.peek().ub;
        }else {//internal node
            Set<indexNode> listnode = root.getNodelist();
            for (indexNode aListNode: listnode){
                double LowerBound = distance(aListNode.getPivot(), query) - aListNode.getEMDRadius();
                double UpperBound = distance(aListNode.getPivot(),query) + aListNode.getEMDRadius();
//                System.out.println("aListNode.getPivot() = "+aListNode.getPivot()[0] +" , " + aListNode.getPivot()[1]);
//                System.out.println( "query = "+query[0]+" , "+query[1]);
//                System.out.print("distance(aListNode.getPivot(), query)==="+distance(aListNode.getPivot(), query)+"   ");
//                System.out.println("aListNode.getEMDRadius()==="+aListNode.getEMDRadius()+"   ");
//                System.out.println("LowerBound ==="+LowerBound);
//                System.out.println("UpperBound ==="+UpperBound);
                if (LowerBound > UB_Branch){}//no sense looking here
                else{// if (LowerBound <= UB_Branch )
                    BranchAndBound(aListNode);
                }
            }

        }
    }
    public static ArrayList<Integer> getBranchAndBoundResultID(indexNode root){
        long startTime = System.currentTimeMillis();   //??????
        ArrayList<Integer> resultID = new ArrayList<>();
        BranchAndBound(root);
//        System.out.println("PQ_Branch.size()==="+PQ_Branch.size());
        while (!PQ_Branch.isEmpty()){
            indexNodeExpand in = PQ_Branch.poll();
            resultID.addAll(in.getin().getpointIdList());
//            System.out.println("LB = "+ in.lb+"  UB==  "+in.ub);
        }
        long endTime = System.currentTimeMillis(); // current time
        System.out.println("firstFilter time==== " + (endTime - startTime) + "ms");
        System.out.println("firstFilter.size()==" + resultID.size());
        return resultID;
    }

//        public static ArrayList<Integer> KNNSearch(indexNode root, int topK){
//        int purnedNumber;
//        ArrayList<Integer> resultID = new ArrayList<>();
//        HashMap<indexNode, Double> hm = new HashMap<>();
//        double min = 100000000;
//        if (root.isLeaf()){
//            double LowerBound = distance(root.getPivot(),query) - queryRadius -root.getEMDRadius();
//            if (hm.size() < k){
//                hm.put(root, LowerBound);
//            }else {
//                indexNode temp = getMinDistNearestID(hm, dimension);
//                if (LowerBound < hm.get(temp)){
//                    hm.remove(temp);
//                    hm.put(root, LowerBound);
//                }
//            }
//        }else{
//            Set<indexNode> listnode = root.getNodelist();
//            for (indexNode aIndexNode: listnode){
//                double LowerBound = distance(aIndexNode.getPivot(),query) - queryRadius -aIndexNode.getEMDRadius();
//                if (LowerBound < min){
//                    min = LowerBound;
//                    topKSearch(aIndexNode, dimension);
//                }else{
//
//                }
//            }
//            }
//        return resultID;
//    }


    public static ArrayList<Integer> getRelaxEMD(ArrayList<Integer> resultID, int k) throws CloneNotSupportedException {
    Map<Integer, Double> map = new TreeMap<>();
    double[] d = new double[k];
    int[] s = new int[k];
    long startTime = System.currentTimeMillis();
    relax_EMD re = new relax_EMD();
    int t = 0;
    for (int i : resultID) { //the resultID store i+1 in IterMatrix[i][j];
        t++;
        signature_t data = getSignature(i);
        double emd = re.ICT(data, querySignature);
        if (t<=k){
            d[t-1] = emd;
            s[t-1] = i;
//            System.out.println("histogram_name[row] = "+histogram_name[row]);
            map.put(i,emd);
        }
        else { //(t>k)
            int[] Index = re.sort(d,s,false);
//            for (int index : Index){
//                System.out.print("index = " + index +"  ");
//            }
            int lastKey = Index[k-1];
//            System.out.println("dataset ID =="+histogram_name[lastKey-1]);
//            for (int key : map.keySet())
//                System.out.print("key ==="+ key + "    ");
            if (emd < map.get(lastKey)){//histogram_name[s[k-1]-1]
                map.remove(lastKey);
                map.put(i,emd);
                s[k-1] = i;
                d[k-1] = emd;
            }
        }
    }
    long endTime2 = System.currentTimeMillis(); //
    System.out.println("secondFilter time==== " + (endTime2 - startTime) + "ms");

    for (int i = 0; i<k; i++){
        System.out.println("Key: " + histogram_name[s[i]-1] + ", Value: " + map.get(s[i]));// + " == "+d[i]
    }

    ArrayList<Integer> firstFlterResult = new ArrayList<>();
    for (int key : map.keySet()){
        firstFlterResult.add(key);
    }
    return firstFlterResult;
}

    public static PriorityQueue<relaxIndexNode> getRelaxEMD2(ArrayList<Integer> resultID) throws CloneNotSupportedException {
        PriorityQueue<relaxIndexNode> PQ_Relax = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        double LB_Relax = Double.MAX_VALUE;
        double UB_Relax = Double.MAX_VALUE;
        long startTime = System.currentTimeMillis();
        relax_EMD re = new relax_EMD();
        minElementEMD me = new minElementEMD();
        int t = 0;
        for (int i : resultID) { //the resultID store i+1 in IterMatrix[i][j];
            t++;
            signature_t data = getSignature(i);
            double[] emd = new double[2];
            emd[0] = re.ICT(data, querySignature);
            emd[1] = me.emd(data, querySignature);
//            System.out.println("emd[0] = "+emd[0]+"  emd[1]"+emd[1]);
            relaxIndexNode in = new relaxIndexNode(i,emd[0],emd[1]);
            if (PQ_Relax.size()<topk){
                PQ_Relax.add(in);
            }else{
                if (PQ_Relax.isEmpty()){

                    PQ_Relax.add(in);
                }
                else if (in.getLb()>UB_Relax){  }//System.out.println("Pruning");
                else if(in.getUb() <LB_Relax){
                    PQ_Relax.poll();
                    PQ_Relax.add(in);
//                while (in.getUb() < LB_Relax){
//                    PQ_Relax.poll();
//                    if (!PQ_Relax.isEmpty()){
//                        LB_Relax = PQ_Relax.peek().lb;
//                        UB_Relax = PQ_Relax.peek().ub;
//                    }else{
//                        break ;
//                    }
//                }
//                PQ_Relax.add(in);
                }else {
                    PQ_Relax.add(in);
                }
            }
            LB_Relax = PQ_Relax.peek().lb;
            UB_Relax = PQ_Relax.peek().ub;
        }
        long endTime2 = System.currentTimeMillis(); //
        System.out.println("secondFilter time==== " + (endTime2 - startTime) + "ms");

//        for (int i = 0; i<firstFlterResult.size(); i++){
//            System.out.println("Key: " + histogram_name[firstFlterResult.get(i)-1] + ", Value: " );// + " == "+d[i]
//        }
        PriorityQueue<relaxIndexNode> reverse = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        while (!PQ_Relax.isEmpty()){
            reverse.add(PQ_Relax.poll());
        }
        System.out.println("secondFilter.size = "+ reverse.size());
        return reverse;
    }
    public static ArrayList getExactEMD(PriorityQueue<relaxIndexNode> secondFilterResult, int topk) throws CloneNotSupportedException{
        //Due to aIntegers.add(id+1) in traverseConvert2, Thus the rowNumber+1 is stored in resultID
        long startTime = System.currentTimeMillis();
        emd_class hh = new emd_class();
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        ArrayList<Integer> resultID = new ArrayList<Integer>();
        while (!secondFilterResult.isEmpty()){
            relaxIndexNode re = secondFilterResult.poll();
            resultID.add(re.resultId);
            if (result.size()<topk){
                signature_t data = getSignature(re.resultId);
                double emd = hh.emd(data, querySignature, null);
                relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                result.add(in);
            }
            else{
                double best = result.peek().lb;
                double LowerBound = re.lb;
                if( LowerBound >= best){
                    break;
                }
                else{// if (LowerBound < best)
                    signature_t data = getSignature(re.resultId);
                    double emd = hh.emd(data, querySignature, null);
                    if (emd < best){
                        relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                        result.poll();
                        result.add(in);
                    }

                }
            }
        }

        long endTime2 = System.currentTimeMillis(); //
        System.out.println("Exact EMD time==== " + (endTime2 - startTime) + "ms");

        ArrayList<String> resultString = new ArrayList<>();
        ArrayList<Integer> topK_ResultID = new ArrayList<>();
        while (!result.isEmpty()){
            relaxIndexNode r = result.poll();
            String s = "getSignatureID: "+r.resultId+",  Key: " + histogram_name[r.resultId-1] + ", Value: " + r.lb;
            resultString.add(s);
            topK_ResultID.add(r.resultId);
        }
        for (int i=resultString.size(); i>0; i--){
            System.out.println(resultString.get(i-1));
        }
//        for (int i = 0; i<resultID.size();i++){
//            System.out.print(histogram_name[resultID.get(i)-1]+"   ");
//        }
        return topK_ResultID; //reverse
    }
    public static void test(){
        int row = 9925;
        emd_class hh = new emd_class();
        signature_t data = getSignature(row-1);
        double emd = hh.emd(querySignature,data, null);
        System.out.println("emd = "+ emd);
        System.out.println(histogram_name.length);
        System.out.println(histogram_name[row-2]);
    }
    public static void testSub_pairWise(int bestID) throws CloneNotSupportedException{
//        sub_pairWise s = new sub_pairWise();
//        s.pairWise(querySignature, getSignature(bestID));
        System.out.println("change change change");
    }

    public static void main(String[] args) throws IOException, CloneNotSupportedException {
        //given query
        emd_class hh = new emd_class();
        getAllData();
        indexNode ballTree = createBallTree();
//        //RangeSearch
//        ArrayList<Integer> resultID = RangeSearch(ballTree);
        System.out.println("===============================================================");
        ArrayList<Integer> firstFlterResult = getBranchAndBoundResultID(ballTree); //branch and bound
        System.out.println("===============================================================");
        PriorityQueue<relaxIndexNode> secondFlterResult = getRelaxEMD2(firstFlterResult);
////        test(resultID);

        System.out.println("getExactEMD ===================================");
        ArrayList topK_ResultID = getExactEMD(secondFlterResult, topk);
        //subEMD
        int n = topK_ResultID.size();
        int bestID =(int) topK_ResultID.get(n-1); //reverse
//        System.out.println("bestID = " + bestID);
//        testSub_pairWise(7);

    }
}


class ComparatorByIndexNodeExpand implements Comparator{ //ordered by distance
    public int compare(Object o1, Object o2){
        indexNodeExpand in1 = (indexNodeExpand)o1;
        indexNodeExpand in2 = (indexNodeExpand)o2;
        return (in2.getLb()-in1.getLb()>0)?1:-1;
    }
}
class ComparatorByRelaxIndexNode implements Comparator { //small->large
    public int compare(Object o1, Object o2){
        relaxIndexNode in1 = (relaxIndexNode)o1;
        relaxIndexNode in2 = (relaxIndexNode)o2;
        return (in2.getLb()-in1.getLb()>0)?1:-1;
    }
}
class ComparatorByRelaxIndexNodeReverse implements Comparator { //reverse
    public int compare(Object o1, Object o2){
        relaxIndexNode in1 = (relaxIndexNode)o1;
        relaxIndexNode in2 = (relaxIndexNode)o2;
        return (in1.getLb()-in2.getLb()>0)?1:-1;
    }
}

