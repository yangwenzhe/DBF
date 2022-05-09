package ballTree.experiment;

import au.edu.rmit.trajectory.clustering.kmeans.indexNode;
import ballTree.ball_tree;
import entity.feature_t;
import entity.indexNodeExpand;
import entity.relaxIndexNode;
import entity.signature_t;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import statisticOfDataset.preProcessing;
import util.*;

import javax.annotation.processing.SupportedSourceVersion;
import java.io.*;
import java.util.*;


public class Main {
    //global variable
    public static int dimension = 2; //dimension
    public static int totalNumberOfLine; //  total row number of query dataset in test.txt
    public static int topk;
    public static double[] query; // histogram of query data
    public static double queryRadius; //radius of query data
    public static double[][] iterMatrix; // all histogram of data lake
    public static double[] ubMove; // each data's ubMove in data lake
    public static int[] histogram_name;// histogram_name[resultID-1] indicate the dataset ID
    public static signature_t querySignature;
    public static ArrayList<HashMap<Long, Double>> histogramList;
    public static HashMap<Integer, ArrayList<double[]>> allHistogram = new HashMap<>();//store all histogram in database except query
    public static int queryDataRowNumber; // number of row querydata in histogram.txt, instead of dataset ID
    public static int resolution ;
    public static ArrayList<Integer> datasetIDList = new ArrayList<>();
    public static String zcurveFilePath;
    public static int decreaseOfResulution = 0;
    public static HashMap<Integer, HashMap<Long,Double>> dataSetMap;
    public static int leaf_Threshold = 5;
    public static int max_Depth = 20;
    public static int total_query_count;
    public static ArrayList<HashMap<Integer,Integer>> resultHashMapList = new ArrayList<>();
    public static HashMap<Integer,Integer> resultHashMap = new HashMap<>();
    public static String Debug_Level = "setTopK";
    public static int totalNumberOfQuery = 10;

    public static void setResolution() throws IOException,CloneNotSupportedException{
        String[] fourDataSetName = new String[]{"argoverse","trackable","identifiable","public"};//
        String datasetName = fourDataSetName[0];
        int[] resolutionList = new int[]{7};//{10,11,12,13,14};//{5,6,7,8,9};
        topk = 10;
        for (int i = 0; i<resolutionList.length; i++){
            resolution = resolutionList[i];
            zcurveFilePath = "data\\"+datasetName+"\\"+datasetName+"-"+resolution+".ser";
            dataSetMap = deSerializationZcurve(zcurveFilePath);
            histogramList = generateHistogram();
            // The generateQuery function randomly generates the query list
            // ArrayList<Integer> arrayList = generateQuery(1, 10000, totalNumberOfQuery);
            int[] queryList = new int[totalNumberOfQuery];
            switch (datasetName){
                case "argoverse": queryList = new int[]{595,91,386,888,378,806,300,317,773,203};
                    totalNumberOfLine = 205942;
                    break;
                case "trackable": queryList = new int[]{3815,13583,13583,1316,384,5182,1236,17564,6423,13331};
                    totalNumberOfLine = 66380;
                    break;
                case "public":queryList = new int[]{9245,5186,15522,3692,10116,6843,14086,7028,3815,13583};
                    totalNumberOfLine =  546193;
                    break;
                case "identifiable":queryList = new int[]{8164,9245,5186,15522,3692,10116,6843,14086,7028,3815};
                    totalNumberOfLine = 235483;
                    break;
            }
            ArrayList<Integer> arrayList = new ArrayList<>();
            for (int t = 0; t<queryList.length; t++){
                arrayList.add(queryList[t]);
            }
            oneLevelIndex(arrayList,"IM_SIG_star");
            oneLevelIndex(arrayList, "ICT");
            oneLevelIndex(arrayList, "tighter_ICT");
            twoLevelIndex(arrayList,"OurAlgorithms");
            //            oneLevelIndex(arrayList, "centroid_WMD");
        }

    }
    public static void setTopK() throws IOException,CloneNotSupportedException {
        String[] fourDataSetName = new String[]{"argoverse","trackable","identifiable","public"};//
        String datasetName = fourDataSetName[0];
        resolution = 13; //Note******************
        int[] topkList = new int[]{5};
        // The generateQuery function randomly generates the query list
        // ArrayList<Integer> arrayList = generateQuery(1, 10000, totalNumberOfQuery);
        int[] queryList = new int[totalNumberOfQuery];
        switch (datasetName){
            // random select query by randomGeneQuery.java
            case "argoverse": queryList = new int[]{595,91,386,888,378,806,300,317,773,203};
                totalNumberOfLine = 205942;
                resolution = 7;
                break;
            case "trackable": queryList = new int[]{3815,13583,13583,1316,384,5182,1236,17564,6423,13331};
            totalNumberOfLine = 66380;
            break;
            case "public":queryList = new int[]{9245,5186,15522,3692,10116,6843,14086,7028,3815,13583};
                totalNumberOfLine =  546193;
            break;
            case "identifiable":queryList = new int[]{8164,9245,5186,15522,3692,10116,6843,14086,7028,3815};
                totalNumberOfLine = 235483;
            break;
        }
        zcurveFilePath ="/home/gr/wzyang/java/Argov/Argov"+"-"+resolution+".ser";  // "data\\"+datasetName+"\\"+datasetName+"-"+resolution+".ser";//
        dataSetMap = deSerializationZcurve(zcurveFilePath);
        histogramList = generateHistogram();
        System.out.println("datasetName ="+datasetName);
        ArrayList<Integer> arrayList = new ArrayList<>();

        for (int i = 0; i<queryList.length; i++){
            arrayList.add(queryList[i]);
        }
        for (int j = 0; j<topkList.length; j++){
            topk = topkList[j];
//            oneLevelIndex(arrayList,"IM_SIG_star");
//            oneLevelIndex(arrayList, "ICT");
//            oneLevelIndex(arrayList, "tighter_ICT");
            twoLevelIndex(arrayList,"OurAlgorithms");
//            oneLevelIndex(arrayList, "removeOneConstraintEMD");
        }

    }
    public static void setScale() throws CloneNotSupportedException,IOException{
        String[] fourDataSetName = new String[]{"argoverse","trackable","identifiable","public"};//
        String datasetName = fourDataSetName[2];
        resolution = 13;
        topk = 10;
        int[] totalNumberOfLineList = new int[5];
        // The generateQuery function randomly generates the query list
        // ArrayList<Integer> arrayList = generateQuery(1, 10000, totalNumberOfQuery);

        int[] queryList = new int[10];
        switch (datasetName){
            case "argoverse": queryList = new int[]{595,91,386,888,378,806,300,317,773,203};
                totalNumberOfLineList = new int[]{40000, 80000, 120000, 160000, 200000};//
                resolution = 7;
                break;
            case "trackable": queryList = new int[]{3815,13583,13583,1316,384,5182,1236,17564,6423,13331};
                totalNumberOfLineList = new int[]{12000, 24000, 36000, 48000, 60000};//{20000, 30000, 40000, 50000, 60000}
                break;
            case "public":queryList = new int[]{9245,5186,15522,3692,10116,6843,14086,7028,3815,13583};
                totalNumberOfLineList = new int[]{100000, 200000, 300000, 400000, 500000};//
                break;
            case "identifiable":queryList = new int[]{8164,9245,5186,15522,3692,10116,6843,14086,7028,3815};
                totalNumberOfLineList = new int[]{50000, 100000, 140000, 180000, 235483};//50000, 100000, 150000, 200000, 235483}
                totalNumberOfLine = 235483;
                break;
        }
        ArrayList<Integer> arrayList = new ArrayList<>();
        for (int i = 0; i<queryList.length; i++){
            arrayList.add(queryList[i]);
          //  System.out.print(queryList[i]+ "  ;   ");
        }
        zcurveFilePath = "data\\"+datasetName+"\\"+datasetName+"-"+resolution+".ser";
        dataSetMap = deSerializationZcurve(zcurveFilePath);
        histogramList = generateHistogram();

        for (int j = 0; j<totalNumberOfLineList.length; j++){
            totalNumberOfLine = totalNumberOfLineList[j];
            System.out.println("totalNumberOfLine = "+totalNumberOfLine+" ; ");
            oneLevelIndex(arrayList,"IM_SIG_star");
            oneLevelIndex(arrayList, "ICT");
            oneLevelIndex(arrayList, "tighter_ICT");
            twoLevelIndex(arrayList,"OurAlgorithms");
            //            oneLevelIndex(arrayList, "centroid_WMD");
        }

    }
    public static void dataSkewCorrectRatio(String datasetName, int levelNum, ArrayList<Integer> arrayList, String Debug) throws CloneNotSupportedException,IOException{
        resultHashMapList.clear();
        ArrayList<HashMap<Integer,Integer>> hmList = new ArrayList<>();
        for (int i = 0 ; i<50; i++){
            HashMap<Integer,Integer> hm = new HashMap<>();
            hmList.add(hm);
        }
        String Path = "/home/gr/wzyang/java/new_add/argov7-topk/"+datasetName+"-sparse.csv";
        try {
            String record = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
            reader.readLine();//
            while ((record = reader.readLine()) != null) {
//                System.out.println("record = "+record);
                String[] fields = record.split(",");
                for (int i = 0; i<50; i++){
                    switch (levelNum) {
                        case 1:
                            hmList.get(i).put(Integer.parseInt(fields[i]) - 1, 0);
                            break;
                        case 2:
                            hmList.get(i).put(Integer.parseInt(fields[i]), 0);
                            break;
                    }
                }
//                hm0.put(Integer.parseInt(fields[0])-1,0);
            }
        }catch (IOException e) {
//            e.printStackTrace();
        }
        for (int i = 0; i<hmList.size(); i++){
            resultHashMapList.add(hmList.get(i));
        }
//        resultHashMapList.add(hm0);
//        resultHashMapList.add(hm1);
        switch (levelNum){
            case 1:
                oneLevelApproaximate(arrayList, Debug);
                break;
            case 2:
                twoLevelApproaximate(arrayList, Debug);
                break;
        }

        double[] percent = new double[resultHashMapList.size()];
        double average_percent = 0.0;
        for (int a=0; a<resultHashMapList.size(); a++){
            int total = 0;
            int correct = 0;
            for (int id:resultHashMapList.get(a).keySet()){
                total++;
                correct+=resultHashMapList.get(a).get(id);
            }
            percent[a] = (double) correct/total;

//                System.out.println("percent["+a+"] = "+percent[a]);
            average_percent+= (double) correct/total;
        }
        average_percent =(double) average_percent/percent.length;
        System.out.println("correct percent ===================="+average_percent);
    }
    public static void dataSkew() throws IOException,CloneNotSupportedException{
        String[] fourDataSetName = new String[]{"trackable","identifiable","public","Argov"};//
        int name = Integer.parseInt(System.getProperty("name"));
        String datasetName = fourDataSetName[name];
        int[] topkList = new int[6];
        resolution = 13;
        int[] queryList = new int[30];
        int[] sparse = new int[50];
        int[] dense = new int[50];
        int[] sparse2 = new int[50];
        switch (datasetName) {
            case "trackable":
                dense = new int[]{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 31, 37, 38, 47, 94, 97, 118, 119, 120, 121, 122, 123, 153, 154, 155, 156, 157, 158, 161, 199, 200,1218, 1219, 1220, 1221, 1222, 1223, 1224, 1225, 1226, 1229};
                sparse = new int[] {5673, 40, 9858, 235, 7008, 6212, 5986, 3982, 5650, 3751, 182, 8702, 2234, 9019, 622, 5320, 1289, 590, 3746, 2479, 5118, 3075, 747, 2002, 1867, 2831, 9165, 5551, 8249, 8839, 1397, 1394, 2160, 975, 3550, 8902, 760, 6734, 483, 84, 5484, 6226, 6318, 3142, 4128, 2179, 3422, 9235, 2655, 2803};
                queryList = dense;
                totalNumberOfLine = 66380;
                topkList =  new int[] {10};//
                break;
            case "identifiable":
//                sparse = new int[]{ 8903, 9287, 8668, 1750, 2606, 8663, 8927, 5387, 4517, 1115, 975, 1809, 6763, 5126, 2515, 2325, 8114, 2902, 9448, 1893, 6942, 239, 7981, 4431, 3153, 531, 1862, 815, 7478, 9141, 4214, 436, 5194, 3388, 6976, 7118, 6114, 6555, 1758, 7819, 4988, 4931, 9516, 4126, 1760, 6983, 5335, 6824, 1579, 6852};
                sparse = new int[]{9516, 4126, 1760, 6983, 5335, 6824, 1579, 6852};
                dense = new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,512, 513, 514, 515, 530, 545, 549, 553, 555, 558, 559, 568, 571, 572, 573, 583, 587, 588, 595, 596,6423, 6455, 6510, 6553, 6579, 6605, 6609, 6631, 6679, 6698};
                queryList = sparse;
                totalNumberOfLine = 235483;
                topkList =  new int[] {10};//
                break;
            case "public":
                dense = new int[]{ 4, 6, 7, 13, 19, 20, 35, 36, 45, 46, 50, 51, 52, 60, 61, 62, 63, 65, 66, 67, 68, 69, 72, 73, 75, 76, 77, 82, 120, 122,502, 503, 504, 507, 512, 513, 514, 517, 518, 520, 521, 526, 527, 528, 529, 531, 532, 533, 534, 535};
                sparse = new int[]{8521, 2151, 9928, 2058, 3827, 9751, 8716, 5038, 9036, 6605, 826, 8556, 4802, 2212, 4833, 789, 3328, 343, 9917, 9205, 6829, 1976, 3154, 3737, 5553, 1691, 2213, 5847, 7897, 7905, 8040, 617, 7437, 5855, 5890, 9421, 5553, 888, 7308, 3487, 4003, 5384, 955, 3722, 9077, 6972, 6856, 9308, 1214, 7804};
                queryList = sparse;
                totalNumberOfLine =  546193;
                topkList =  new int[] {10};//
                break;
            case "Argov":
                dense = new int[]{128, 142, 318, 338, 346, 580, 591, 614, 633, 676, 731, 833, 857, 954, 1113, 1114, 1135, 1136, 1148, 1159, 1185, 1312, 1369, 1404, 1415, 1586, 1701, 1771, 1809, 1815, 1866, 1903, 2032, 2037, 2088, 2122, 2229, 2380, 2569, 2615, 2634, 2798, 2842, 2858, 3038, 3128, 3193, 3273, 3311, 3343};
                sparse = new int[]{105,10,866,206,594,395,549,305,394,493,475,71,439,719,777,168,103,982,649,540, 7247, 8088, 4039, 9489, 7933, 1707, 2455, 3276, 3417, 8043, 6371, 8405, 5169, 5745, 8572, 5338, 9948, 2056, 200, 3249, 8332, 8644, 3063, 2435, 5603, 1485, 2131, 8548, 4194, 4999};
                queryList = dense;
                totalNumberOfLine = 205942;
                topkList =  new int[] {10};//
                resolution = 7;
                break;
        }
        total_query_count = queryList.length;
        ArrayList<Integer> arrayList = new ArrayList<>();
        for (int i = 0; i<queryList.length; i++){
            arrayList.add(queryList[i]);
        }
        zcurveFilePath = "data\\"+datasetName+"\\"+datasetName+"-"+resolution+".ser";
        dataSetMap = deSerializationZcurve(zcurveFilePath);
        histogramList = generateHistogram();

        for (int j = 0; j<topkList.length; j++) {
            resultHashMapList.clear();
            topk = topkList[j];
//            oneLevelIndex(arrayList, "ICT");
//            oneLevelIndex(arrayList, "tighter_ICT");
//            twoLevelIndex(arrayList,"OurAlgorithms");
//            oneLevelIndex(arrayList,"IM_SIG_star");
            dataSkewCorrectRatio(datasetName,2,arrayList, "tighter_ICT");
            dataSkewCorrectRatio(datasetName,1,arrayList, "tighter_ICT");
            dataSkewCorrectRatio(datasetName,1, arrayList, "ICT");
            dataSkewCorrectRatio(datasetName,1,arrayList,"IM_SIG_star");
        }

    }

    public static void main(String[] args)  throws IOException, CloneNotSupportedException, FileNotFoundException {
        switch (Debug_Level){
            case "setResolution": setResolution();break;
            case "setTopK": setTopK();break;
            case "setScale": setScale();break;
            case "dataSkew": dataSkew();break;
            default: break;
        }
    }

    public static void twoLevelIndex(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException,CloneNotSupportedException, FileNotFoundException {
        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++){
            queryDataRowNumber = query.get(i);
            System.out.print( "queryDataset  =  " +query.get(i)+" ; ");
            long startTime = System.currentTimeMillis();
            ArrayList<Integer> datasetID = getAllData();
            long endTime1 = System.currentTimeMillis(); //
            indexNode ballTree = createBallTree();
            long endTime2 = System.currentTimeMillis(); //
            ArrayList<Integer> firstFlterResult = getBranchAndBoundResultID(ballTree); //branch and bound
            long FirstFilterTime = System.currentTimeMillis();
            PriorityQueue<relaxIndexNode> secondFlterResult = getRelaxEMD(firstFlterResult);
            long SecondFilterTime = System.currentTimeMillis();
            ArrayList topK_ResultID = getExactEMD(secondFlterResult, topk);
            long VerifineTime = System.currentTimeMillis();
            long endTime3 = System.currentTimeMillis(); //
            System.out.print("create_tree = "+(endTime2 - endTime1)+"; ");
            System.out.print("FilterTime = "+(SecondFilterTime-endTime2)+"; ");
            System.out.print("VerifineTime = " +(VerifineTime-SecondFilterTime)+"; ");
            System.out.println("total_time = "+(endTime3 - endTime1));
            time[i] = (endTime3 - endTime1);
        }

        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/10;
        System.out.println("average_time =  "+averageTime);

    }

    public static void oneLevelIndex(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException,CloneNotSupportedException,FileNotFoundException {

        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++){
            queryDataRowNumber = query.get(i);
            long start = System.currentTimeMillis();
            ArrayList<Integer> datasetID = getAllData();
            long startTime = System.currentTimeMillis(); //
            PriorityQueue<relaxIndexNode> ICTresult = sortByLowerBound(datasetID, DEBUG_LEVEL);
            long FilterTime = System.currentTimeMillis();
            ArrayList topK_ResultID = getExactEMD_ICT(ICTresult, topk);
            long VerifineTime = System.currentTimeMillis();

            long endTime2 = System.currentTimeMillis(); //
            System.out.print("FilterTime = "+(FilterTime-startTime)+";  ");
            System.out.print("VerifineTime = "+(VerifineTime-FilterTime)+";  ");
            System.out.println("totalTime = "+(endTime2 - startTime));
            time[i] = (endTime2 - startTime);
        }
        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/10;
        System.out.println("average_time =  "+averageTime);
    }

    public static void oneLevelApproaximate(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException, CloneNotSupportedException{
        System.out.println("Approaximate  topK ; "+"DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++){
            queryDataRowNumber = query.get(i);
            long startTime = System.currentTimeMillis();
            ArrayList<Integer> datasetID = getAllData();
            long endTime1 = System.currentTimeMillis(); //

            ArrayList<Integer> resultID = new ArrayList<>();
            for (int t = 0; t<datasetID.size(); t++){
                resultID.add(datasetID.get(t));
            }
            PriorityQueue<relaxIndexNode> PQ_Relax = new PriorityQueue<>(new ComparatorByRelaxIndexNode());//small->large
            PriorityQueue<relaxIndexNode> approxResult = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
            relax_EMD re = new relax_EMD();
            int filterCount = 0;
            int refineCount = 0;

            for (int id : resultID){
                signature_t data = getSignature(id);
                double[] emd = new double[2];
                switch (DEBUG_LEVEL){
                    case "ICT":emd[0] = re.ICT(data, querySignature);
                        break;
                    case "tighter_ICT":emd[0] = re.tighter_ICT(data, querySignature);
                        break;
                    case "centroid_WMD":emd[0]= re.centroid_WMD(data, querySignature);
                        break;
                    case "removeOneConstraintEMD":emd[0] = re.removeOneConstraintEMD(data, querySignature);
                        break;
                    case "IM_SIG_star":emd[0] = re.IM_SIG_star(data, querySignature);
                        break;
                    default: System.out.println("??????????????????");
                        break;
                }

                emd[1] = 0;
                relaxIndexNode in = new relaxIndexNode(id,emd[0],emd[1]);
                PQ_Relax.add(in);
                if (approxResult.size() < topk){
                    approxResult.add(in);
                    refineCount++;
                }else{
                    double best = approxResult.peek().getLb();
                    if (in.getLb() >= best){ filterCount++; continue; }
                    else{
                        approxResult.poll();
                        approxResult.add(in);
                        refineCount++;
                    }

                }
            }

            int count = 0;
//            while (!approxResult.isEmpty()&& count<topk) {
//                relaxIndexNode r= approxResult.poll();
//                System.out.println(r.resultId+": "+datasetIDList.get(histogram_name[r.resultId])+".csv  ,  "+r.getLb());// +";  n = " +r.getUb()
//            }

                long endTime3 = System.currentTimeMillis(); //
            System.out.println("total_time = "+(endTime3 - endTime1)+"; ");
            time[i] = (endTime3 - endTime1);
        }


        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/10;
        System.out.println("average_time =  "+averageTime);
//        return resultApprox;
    }
    public static void twoLevelApproaximate(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException, CloneNotSupportedException{
        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++){
            queryDataRowNumber = query.get(i);
            long startTime = System.currentTimeMillis();
            ArrayList<Integer> datasetID = getAllData();

            long endTime1 = System.currentTimeMillis(); //
            indexNode ballTree = createBallTree();
            long endTime2 = System.currentTimeMillis(); //

            ArrayList<Integer> firstFlterResult = getBranchAndBoundResultID(ballTree); //branch and bound
            long FirstFilterTime = System.currentTimeMillis();
            PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
            relax_EMD re = new relax_EMD();

            int filterCount = 0;
            int refineCount =0;

            for (int id: firstFlterResult){
                signature_t data = getSignature(id-1);
                double emdLB = re.tighter_ICT(data, querySignature);
                if (resultApprox.size()<topk){
                    relaxIndexNode in = new relaxIndexNode(id, emdLB);
                    resultApprox.add(in);
                    refineCount++;
                }else{
                    double best = resultApprox.peek().lb;
                    double lowerbound = emdLB;
                    if (lowerbound >= best){ filterCount++; continue;}//被过滤
                    else {
                        relaxIndexNode in = new relaxIndexNode(id,emdLB);
                        resultApprox.poll();
                        resultApprox.add(in);
                        refineCount++;
                    }
                }
            }

            //show result
//            int count = 0;
//            while (!resultApprox.isEmpty()&& count<topk){
//                //reverse.add(PQ_Relax.poll());
//                relaxIndexNode r= resultApprox.poll();
//                System.out.println(r.resultId+": "+datasetIDList.get(histogram_name[r.resultId-1])+".csv  ,  "+r.getLb()+";  ");
//                count++;
//            }

            System.out.print("firstFlterCount = "+(datasetID.size()- firstFlterResult.size())+"; ");
            System.out.print("secondfilterCount =" + filterCount+"; ");
//            System.out.print("refineCount = "+ refineCount+"; ");

            long endTime3 = System.currentTimeMillis(); //
            System.out.print("create_tree = "+(endTime2 - endTime1)+"; ");
            System.out.println("total_time = "+(endTime3 - endTime1)+"; ");
            time[i] = endTime3 - endTime1;

        }
        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/10;
        System.out.println("average_time =  "+averageTime);


    }
    public static ArrayList<HashMap<Long, Double>> generateHistogram() throws CloneNotSupportedException{
        ArrayList<HashMap<Long, Double>> histogram = new ArrayList<>();
        if (decreaseOfResulution == 0){
            for (int id: dataSetMap.keySet()){
                HashMap<Long, Double> map1 = dataSetMap.get(id);
                histogram.add(map1);
                int name = id;
                datasetIDList.add(name);
            }
        }else if(decreaseOfResulution > 0){
            for (int id: dataSetMap.keySet()){
                HashMap<Long, Double> map1 = dataSetMap.get(id);
                long []Coordinates;
                HashMap<Long, Double> hm = new HashMap<Long, Double>();
                for (long key: map1.keySet()){
                    Coordinates = readZcurve.resolve(key);
                    double weight = map1.get(key);
                    long[] transferCoordinates = new long[2];
                    long t =(long)Math.pow(2,decreaseOfResulution);
                    transferCoordinates[0] = Coordinates[0]/t ;
                    transferCoordinates[1] = Coordinates[1]/t ;
                    long zcode = storeZcurve.combine(transferCoordinates[0],transferCoordinates[1],resolution+decreaseOfResulution);
                    if (hm.get(zcode) == null){
                        hm.put(zcode, weight);
                    }else{
                        double temp = hm.get(zcode);
                        hm.put(zcode,weight+temp);
                    }
                }
                histogram.add(hm);
                int name = id;
                datasetIDList.add(name);
            }
        }else {
            System.out.println("decreaseOfResulution < 0");
        }
        return histogram;
    }


    public static ArrayList<String[]> getPooling() throws CloneNotSupportedException, IOException {
        readZcurve rz = new readZcurve();
        pooling p = new pooling();
        ArrayList<String[]> dataSetList_after_pooling = new ArrayList<String[]>();
        for (int id=0; id<histogramList.size(); id++){
            HashMap<Long, Double> map1 = histogramList.get(id);
            signature_t s1 = getSignature(map1);
            signature_t s1_pooling = p.poolingOperature(s1, 1);
//            System.out.println("id = "+id+"   map1.n = "+map1.size()+"   s1.n  = "+s1.n+"   s1_pooling.n = "+s1_pooling.n);
            double ub_move = p.getUb();

            String[] string = new String[4];

            string[0] = String.valueOf(id);
            string[1] = String.valueOf(s1_pooling.Features[0].X);
            string[2] = String.valueOf(s1_pooling.Features[0].Y);
            string[3] = String.valueOf(ub_move);

            dataSetList_after_pooling.add(string);

        }
        return dataSetList_after_pooling;
    }

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

    public static ArrayList<Integer> getAllData() throws IOException,CloneNotSupportedException {
        //sampleData
        ArrayList<double[]> l = new ArrayList<>();
        DoubleArrayList ub = new DoubleArrayList();
        ArrayList<String[]> datasetList_after_pooling = getPooling();
        ArrayList<Integer> his = new ArrayList(); //his coresponding to the all histogram_name
        ArrayList<Integer> datasetID = new ArrayList<>();
        int numberOfLine = 0;
        while (numberOfLine< totalNumberOfLine && numberOfLine<histogramList.size()){
            ArrayList<double[]> allPoints = new ArrayList<>();
            HashMap<Long, Double> map1 = histogramList.get(numberOfLine);
            for (long key: map1.keySet()){
                long[] coord = readZcurve.resolve(key);
                double [] d = new double[3];
                d[0] = Double.parseDouble(String.valueOf(coord[0]));
                d[1] = Double.parseDouble(String.valueOf(coord[1]));
                d[2] = map1.get(key);
                allPoints.add(d);
            }
            //sampleData
            String[] buf = datasetList_after_pooling.get(numberOfLine);
            allHistogram.put(numberOfLine, allPoints);
            datasetID.add(numberOfLine);
            if (numberOfLine +1 == queryDataRowNumber){
                //sampleData
                query = new double[dimension];
                query[0] = Double.parseDouble(buf[1]);
                query[1] = Double.parseDouble(buf[2]);
                queryRadius = Double.parseDouble(buf[3]);

                int n = allPoints.size();
                feature_t[] features = new feature_t[n];
                double[] weights = new double[n];
                for (int i = 0; i < n; i++) {
                    features[i] = new feature_t(allPoints.get(i)[0], allPoints.get(i)[1]);
                    weights[i] = allPoints.get(i)[2];
                }
                querySignature = new signature_t(n, features, weights);
            }
            //sampleData
            double[] corrd = new double[dimension];
            corrd[0] = Double.parseDouble(buf[1]);
            corrd[1] = Double.parseDouble(buf[2]);
            l.add(corrd);
            his.add(Integer.parseInt(buf[0]));
            ub.add(Double.parseDouble(buf[3]));
            numberOfLine++;

        }
        //sampleData
        int countOfRow = numberOfLine;
        iterMatrix = new double[countOfRow][dimension];
        ubMove = new double[countOfRow];
        histogram_name = new int[countOfRow];
        ubMove = ub.toDoubleArray();
        for (int i = 0; i < countOfRow; i++) {
            iterMatrix[i][0] = l.get(i)[0];
            iterMatrix[i][1] = l.get(i)[1];
            histogram_name[i] = his.get(i);
        }
        return datasetID;
    }

    public static indexNode createBallTree() throws IOException,CloneNotSupportedException {
//        long startTime = System.currentTimeMillis();
        int leafThreshold = leaf_Threshold;
        int maxDepth = max_Depth; // 2^22 = 104 8576
        long startTime = System.currentTimeMillis();
        long endTime1 = System.currentTimeMillis();
        indexNode in = ball_tree.create(ubMove, iterMatrix, leafThreshold, maxDepth, dimension);
        long endTime2 = System.currentTimeMillis(); // current time
       return in;
    }

    public static double distance(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return Math.sqrt(d);
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
                if (LowerBound > UB_Branch){}//no sense looking here
                else{// if (LowerBound <= UB_Branch )
                    BranchAndBound(aListNode);
                }
            }

        }
    }
    public static ArrayList<Integer> getBranchAndBoundResultID(indexNode root){
        ArrayList<Integer> resultID = new ArrayList<>();
        BranchAndBound(root);
        while (!PQ_Branch.isEmpty()){
            indexNodeExpand in = PQ_Branch.poll();
            resultID.addAll(in.getin().getpointIdList());
//            System.out.println("LB = "+ in.lb+"  UB==  "+in.ub);
        }
        return resultID;
    }

    public static PriorityQueue<relaxIndexNode> getRelaxEMD(ArrayList<Integer> resultID) throws CloneNotSupportedException {
        PriorityQueue<relaxIndexNode> PQ_Relax = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        relax_EMD re = new relax_EMD();
        for (int i : resultID) { //the resultID store i+1 in IterMatrix[i][j];
            signature_t data = getSignature(i-1);
            double[] emd = new double[2];
            emd[0] = re.tighter_ICT(data, querySignature);
            emd[1] = 0;
            relaxIndexNode in = new relaxIndexNode(i,emd[0],emd[1]);
            PQ_Relax.add(in);
        }
        PriorityQueue<relaxIndexNode> reverse = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        while (!PQ_Relax.isEmpty()){
            reverse.add(PQ_Relax.poll());
        }
        return reverse;
    }

    public static PriorityQueue<relaxIndexNode> getRelaxEMD2(ArrayList<Integer> resultID) throws CloneNotSupportedException {
        PriorityQueue<relaxIndexNode> PQ_Relax = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        double LB_Relax = Double.MAX_VALUE;
        double UB_Relax = Double.MAX_VALUE;
        long startTime = System.currentTimeMillis();
        relax_EMD re = new relax_EMD();
//        minElementEMD me = new minElementEMD();
        int t = 0;
        for (int i : resultID) { //the resultID store i+1 in IterMatrix[i][j];
            t++;
            signature_t data = getSignature(i-1);
            double[] emd = new double[2];
            emd[0] = re.tighter_ICT(data, querySignature);
            emd[1] = re.upper_bound_test_ICT(data, querySignature);
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


    public static ArrayList getExactEMD_ICT(PriorityQueue<relaxIndexNode> secondFilterResult, int topk) throws CloneNotSupportedException{
        long startTime = System.currentTimeMillis();
        emd_class hh = new emd_class();
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        ArrayList<Integer> resultID = new ArrayList<Integer>();

        int refineCount = 0;
        while (!secondFilterResult.isEmpty()){
            relaxIndexNode re = secondFilterResult.poll();
            resultID.add(re.resultId);
            if (result.size()<topk){
                signature_t data = getSignature(re.resultId);
                double emd = hh.emd(data, querySignature, null);
                relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                result.add(in);
                refineCount++;
            }
            else{
                double best = result.peek().lb;
                double LowerBound = re.lb;
                if( LowerBound >= best){
                    break;
                }
                else{// if (LowerBound < best)
                    signature_t data = getSignature(re.resultId);
                    refineCount++;
                    double emd = hh.emd(data, querySignature, null);
                    if (emd < best){
                        relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                        result.poll();
                        result.add(in);
                    }

                }
            }
        }

        ArrayList<Double> topK_EMD = new ArrayList<>();
        ArrayList<String> resultString = new ArrayList<>();
        ArrayList<Integer> topK_ResultID = new ArrayList<>();
        while (!result.isEmpty()){
            relaxIndexNode r = result.poll();
            String s = "r.resultId: "+r.resultId +",  "+datasetIDList.get(histogram_name[r.resultId])+".csv" + ", Value: " + r.lb+ ", lb: " + r.ub;
            resultString.add(s);
            topK_ResultID.add(r.resultId);
            topK_EMD.add(r.lb);
        }

//        for (int j = 0; j<resultString.size(); j++){
//            System.out.println(j+" , "+resultString.get(j));
//        }  // detail

        return topK_ResultID; //reverse
    }

    public static ArrayList getExactEMD(PriorityQueue<relaxIndexNode> secondFilterResult, int topk) throws CloneNotSupportedException{
        int refineCount = 0;
        emd_class hh = new emd_class();
        PriorityQueue<relaxIndexNode> result = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        ArrayList<Integer> resultID = new ArrayList<Integer>();
        while (!secondFilterResult.isEmpty()){
            relaxIndexNode re = secondFilterResult.poll();
            resultID.add(re.resultId);
            if (result.size()<topk){
                signature_t data = getSignature(re.resultId-1);
                double emd = hh.emd(data, querySignature, null);
                relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                result.add(in);
                refineCount++;
            }
            else{
                double best = result.peek().lb;
                double LowerBound = re.lb;
                if( LowerBound >= best){
                    break;
                }
                else{// if (LowerBound < best)
                    signature_t data = getSignature(re.resultId-1);
                    double emd = hh.emd(data, querySignature, null);
                    refineCount++;
                    if (emd < best){
                        relaxIndexNode in = new relaxIndexNode(re.resultId, emd);
                        result.poll();
                        result.add(in);
                    }

                }
            }
        }

        ArrayList<String> resultString = new ArrayList<>();
        ArrayList<Integer> topK_ResultID = new ArrayList<>();
        ArrayList<Double> topK_EMD = new ArrayList<>();
        while (!result.isEmpty()){
            relaxIndexNode r = result.poll();
            String s = "r.resultId: "+r.resultId+",  "+datasetIDList.get(histogram_name[r.resultId-1])+".csv" + ",  Value: " + r.lb+ ", lb: " + r.ub;
//            System.out.println(r.resultId+", "+datasetIDList.get(histogram_name[r.resultId-1])+".csv  ,  "+r.getLb()+"; ");
            resultString.add(s);
            topK_ResultID.add(r.resultId);
            topK_EMD.add(r.lb);
        }

        return topK_ResultID; //reverse
    }
    public static PriorityQueue<relaxIndexNode> sortByLowerBound(ArrayList<Integer> resultID, String DEBUG_LEVEL) throws CloneNotSupportedException {
        PriorityQueue<relaxIndexNode> PQ_Relax = new PriorityQueue<>(new ComparatorByRelaxIndexNode());//small->large
        relax_EMD re = new relax_EMD();
        for (int i : resultID){
            signature_t data = getSignature(i);
            double[] emd = new double[2];
            switch (DEBUG_LEVEL){
                case "ICT":emd[0] = re.ICT(data, querySignature);
                    break;
                case "tighter_ICT":emd[0] = re.tighter_ICT(data, querySignature);
                    break;
                case "centroid_WMD":emd[0]= re.centroid_WMD(data, querySignature);
                    break;
                case "removeOneConstraintEMD":emd[0] = re.removeOneConstraintEMD(data, querySignature);
                    break;
                case "IM_SIG_star":emd[0] = re.IM_SIG_star(data, querySignature);
                    break;
                default: System.out.println("??????????????????");
                    break;
            }
            emd[1] = 0;
            relaxIndexNode in = new relaxIndexNode(i,emd[0],emd[1]);
            PQ_Relax.add(in);
        }

        PriorityQueue<relaxIndexNode> reverse = new PriorityQueue<>(new ComparatorByRelaxIndexNodeReverse());
        while (!PQ_Relax.isEmpty()){
            reverse.add(PQ_Relax.poll());
        }
        return reverse;
    }


    public static ArrayList<Integer> generateQuery(int min, int max, int totalNumberOfQuery){
        ArrayList<Integer> arrayList = new ArrayList<>();
        Random random = new Random();
        while (arrayList.size() < totalNumberOfQuery){
            int s = random.nextInt(max)%(max-min+1) + min;
            arrayList.add(s);
        }
        return arrayList;
    }


    public static HashMap<Integer, HashMap<Long,Double>> deSerializationZcurve(String file) {
        HashMap<Integer, HashMap<Long,Double>> result;
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            result = (HashMap) ois.readObject();
            ois.close();
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Class not found");
            c.printStackTrace();
            return null;
        }
        return result;
    }

    public static signature_t getSignature(HashMap<Long,Double> map){
        int n = map.size();
        feature_t[] Features = new feature_t[n];
        double []Weights = new double[n];
        long []Coordinates;
        double unit = 360/Math.pow(2,resolution);
        int i=0;
        for(long key:map.keySet()){
            Coordinates = readZcurve.resolve(key);
            Features[i] = new feature_t(Coordinates[0],Coordinates[1]);
            Weights[i] = map.get(key);
            i++;
        }
        signature_t s = new signature_t(n,Features,Weights);
        return s;
    }

}

class ComparatorByIndexNodeExpand implements Comparator { //ordered by distance
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