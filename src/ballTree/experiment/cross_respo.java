package ballTree.experiment;

import au.edu.rmit.trajectory.clustering.kmeans.indexNode;
import ballTree.ball_tree;
import entity.feature_t;
import entity.indexNodeExpand;
import entity.relaxIndexNode;
import entity.signature_t;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.omg.CORBA.INTERNAL;
import util.*;

import java.io.*;
import java.util.*;

public class cross_respo {

    //global variable
    public static double minX = -90; //-120;
    public static double minY = -180; //500;
    public static double range = 360;
    public static int dimension = 2; //dimension
    public static int totalNumberOfLine; //  total row number of query dataset in test.txt
    public static int topk;
    public static double[] query; // histogram of query data
    public static double queryRadius; //radius of query data
    public static double[][] iterMatrix; // all histogram of data lake
    public static double[] ubMove; // each data's ubMove in data lake
    public static int[] histogram_name;// histogram_name[resultID-1] indicate the dataset ID
    public static signature_t querySignature;
    //Integer is row of iterMatrix, ArrayList is corrdinate of each bin in histogram
    public static ArrayList<HashMap<Long, Double>> histogramList;
    public static ArrayList<HashMap<Long, Double>> threeHistogramList = new ArrayList<>();

    public static HashMap<Integer, ArrayList<double[]>> allHistogram = new HashMap<>();//store all histogram in database except query
    public static int queryDataRowNumber; // number of row querydata in histogram.txt, instead of dataset ID
    //    public static String DEBUG_LEVEL = "OurAlgorithms";//1,ICT  2,tighter_ICT  3,centroid_WMD  4,removeOneConstraintEMD 5,OurAlgorithms
    public static String storeFilePath;
    public static String zcurveFilePath;
    public static int decreaseOfResulution = 0;
    public static HashMap<Integer, HashMap<Long,Double>> dataSetMap;
    public static int total_query_count;
    public static int leaf_Threshold = 5;
    public static int max_Depth = 20; // 16; // 18; //
    public static int[] resolutionList = {13};
    public static int resolution;
    public static ArrayList<String> datasetIDList = new ArrayList<>();
    public static void generateThreeHistogram(HashMap<Integer, HashMap<Long,Double>> datasetmap, int i) throws CloneNotSupportedException{
        if (decreaseOfResulution == 0){
            for (int id: datasetmap.keySet()){
                HashMap<Long, Double> map1 = datasetmap.get(id);
                threeHistogramList.add(map1);
                String s = String.valueOf(id);
                String prefix = String.valueOf(i);
                String name = prefix+"-"+s;
                datasetIDList.add(name);
            }
        }else if(decreaseOfResulution > 0){
            for (int id: datasetmap.keySet()){
                HashMap<Long, Double> map1 = datasetmap.get(id);
                long []Coordinates;
                HashMap<Long, Double> hm = new HashMap<Long, Double>();
                for (long key: map1.keySet()){
                    Coordinates = readZcurve.resolve(key);//?zcode?????????
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
                threeHistogramList.add(hm);
                String s = String.valueOf(id);
                String prefix = String.valueOf(i);
                String name = prefix+"-"+s;
                datasetIDList.add(name);
            }
        }else {
            System.out.println("decreaseOfResulution < 0");
        }
    }


    public static void three_repository() throws IOException,CloneNotSupportedException{
        int[] topkList = new int[]{5,10,15,20,25,30};//{10,20,30,40,50,60};//
        for (int t = 0; t<resolutionList.length;t++){
            resolution = resolutionList[t];
            int[] queryList = {595,91,386,888,378,806,300,317,773,203};
            //{ 2872, 2071, 6632, 1865, 506, 2191, 8900, 3963, 9362, 1148};//iden combine
            //{595,91,386,888,378,806,300,317,773,203};//pub
            //2, 3815,13583,1316,384,5182,1236,17564,6423,13331 ;//track:

            total_query_count = queryList.length;
            ArrayList<Integer> arrayList = new ArrayList<>();
            for (int i = 0; i<queryList.length; i++){
                arrayList.add(queryList[i]);
            }
            String[] threeDataSetName = new String[]{"trackable","identifiable","public"};//
            for (int i = 0; i<threeDataSetName.length; i++){
                String datasetName = threeDataSetName[i];
                zcurveFilePath = "/home/gr/wzyang/java/"+datasetName+"/"+datasetName+"-"+resolution+".ser";
                HashMap<Integer, HashMap<Long,Double>> dataSetMap = deSerializationZcurve(zcurveFilePath);
                generateThreeHistogram(dataSetMap, i);
            }
            totalNumberOfLine = (int)(66380+546193+235483);
            System.out.println("totalNumberOfLine = "+totalNumberOfLine);
            histogramList = threeHistogramList;
            for (int j = 0; j<topkList.length; j++) {
                topk = topkList[j];
                oneLevelIndex(arrayList, "ICT");
                oneLevelIndex(arrayList, "tighter_ICT");
                oneLevelIndex(arrayList, "IM_SIG_star");
                twoLevelIndex(arrayList,"OurAlgorithms");
            }
        }
    }

    public static void cross_query()throws IOException,CloneNotSupportedException{
        //random choose query dataset from trackable
        for (int t =0; t<resolutionList.length; t++){
            resolution = resolutionList[t];
            // random select query datasets
            int[] queryList =  { 2872, 2071, 6632, 1865, 506, 2191, 8900, 3963, 9362, 1148};
            total_query_count = queryList.length;
            ArrayList<Integer> arrayList = new ArrayList<>();
            for (int i = 0; i<queryList.length; i++){
                arrayList.add(queryList[i]);
            }
            String[] fourDataSetName = new String[]{"trackable","identifiable","public"};//
            int name = Integer.parseInt(System.getProperty("name"));
            String datasetName = fourDataSetName[name];
            int[] topkList = new int[]{10,20,30,40,50,60};//
            zcurveFilePath = "/home/gr/wzyang/java/"+datasetName+"/"+datasetName+"-"+resolution+".ser";
            switch (datasetName) {
                case "trackable":
                    totalNumberOfLine = 66380;
                    break;
                case "public":
                    totalNumberOfLine =  546193;
                    break;
                case "identifiable":
                    totalNumberOfLine = 235483;
                    break;
            }
            dataSetMap = deSerializationZcurve(zcurveFilePath);
            histogramList = generateHistogram();
            for (int j = 0; j<topkList.length; j++) {
                topk = topkList[j];
                oneLevelIndex(arrayList, "ICT");
                oneLevelIndex(arrayList, "tighter_ICT");
                oneLevelIndex(arrayList, "IM_SIG_star");
                twoLevelIndex(arrayList,"OurAlgorithms");
            }
        }

    }

    public static void generateQuery(int queryDataRowNumber) throws CloneNotSupportedException{
        String[] fourDataSetName = new String[]{"trackable","identifiable","public"};//
        int queryName = Integer.parseInt(System.getProperty("queryname"));
        String Path = "/home/gr/wzyang/gpx-planet/"+fourDataSetName[queryName]+"/"+queryDataRowNumber+".csv";
        double blockSize = range/Math.pow(2,resolution);
        HashMap<Long,Integer> ZorderHis = new HashMap<>();
        long t = (long)Math.pow(2,resolution);
        try {
            String record = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
            while((record=reader.readLine())!=null){
                String[] fields = record.split(",");
                double x = Double.parseDouble(fields[0]) - minX;
                double y = Double.parseDouble(fields[1]) - minY;
                long row = (long) (y/blockSize);
                long col = (long) (x/blockSize);
                long id = row*t+col;
                Integer num = ZorderHis.getOrDefault(id,new Integer(0));
                ZorderHis.put(id,num+1);
            }
            reader.close();
            //        System.out.println("Zorder Histogram ====");
            int totalCount = 0;
            for (long id:ZorderHis.keySet()){
                totalCount += ZorderHis.get(id);
            }
            HashMap<Long,Double> ZorderDensityHist = new HashMap<>();
            for (long id:ZorderHis.keySet()){
//                System.out.print("id = "+id+",  ZorderHis.get(id) = " +ZorderHis.get(id));
                double yyy = (double)ZorderHis.get(id)/totalCount;
//                System.out.println("  yyy = "+yyy);
                ZorderDensityHist.put(id, yyy);
            }

            ArrayList<double[]> allPoints = new ArrayList<>();
            for (long key: ZorderDensityHist.keySet()){
                double [] d = new double[3];
                d[0] = (int)(key%Math.pow(2,resolution)); //x
                d[1] = (int)(key/Math.pow(2,resolution));  //y
                d[2] = ZorderDensityHist.get(key);
                allPoints.add(d);
            }
            //1, generate the querySignature
            int n = allPoints.size();
//            System.out.println("querySignature.n = " + n);
            feature_t[] features = new feature_t[n];
            double[] weights = new double[n];
            for (int i = 0; i < n; i++) {
                features[i] = new feature_t(allPoints.get(i)[0], allPoints.get(i)[1]);
                weights[i] = allPoints.get(i)[2];
//                System.out.println(allPoints.get(i)[0]+", "+allPoints.get(i)[1]+", "+allPoints.get(i)[2]);
            }
            querySignature = new signature_t(n, features, weights);
            //1,pooling and obtain the query Radius
            pooling p = new pooling();
            signature_t query_pooling = p.poolingOperature(querySignature,1);
            double ub_move = p.getUb();
            query = new double[dimension];
//            System.out.println("x = "+query_pooling.Features[0].X+", y = "+query_pooling.Features[0].Y);
            query[0] = query_pooling.Features[0].X;
            query[1] = query_pooling.Features[0].Y;
            queryRadius = ub_move;

        }catch (IOException e) {
//            e.printStackTrace();
            System.out.println("this dataset name isn't exist");
        }
    }

    public static void dataSkew() throws IOException,CloneNotSupportedException{

    }

    public static void main(String[] args)throws CloneNotSupportedException,IOException{
//        ArrayList<Integer> arrayList = generateQuery(1,1000, 20);
//        for (int i = 0; i<arrayList.size(); i++){
//            System.out.println(arrayList.get(i));
//        }
//        cross_query();
        three_repository();

    }

    public static void twoLevelIndex(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException,CloneNotSupportedException, FileNotFoundException{
        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++) {
            queryDataRowNumber = query.get(i);
            System.out.print("queryDataRowNumber = "+queryDataRowNumber+" ; ");
            generateQuery(queryDataRowNumber);

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
            //            System.out.println("create_tree = "+(endTime2 - endTime1)+";   "+"total_time = "+(endTime3 - endTime1));//
//            System.out.println("preprocessing = "+(endTime1 - startTime));
            System.out.print("create_tree = "+(endTime2 - endTime1)+"; ");
            System.out.print("FilterTime = "+(SecondFilterTime-endTime2)+"; ");
//            System.out.println("FirstFilter = " +(FirstFilterTime-endTime2));
//            System.out.println("SecondFilter = " +(SecondFilterTime-FirstFilterTime));
            System.out.print("VerifineTime = " +(VerifineTime-SecondFilterTime)+"; ");
            System.out.println("total_time = "+(endTime3 - endTime1));
//            System.out.println((endTime3 - endTime1));
            time[i] = (endTime3 - endTime1);
        }
        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/total_query_count;
        System.out.println("average_time =  "+averageTime);


    }


    public static void twoLevelIndexApproximate(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException,CloneNotSupportedException, FileNotFoundException{
        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++){
            queryDataRowNumber = query.get(i);
            System.out.print( "queryDataRowNumber  =  " +query.get(i)+" ; ");
            generateQuery(queryDataRowNumber);

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
                    if (lowerbound >= best){ filterCount++; continue;}//???
                    else {
                        relaxIndexNode in = new relaxIndexNode(id,emdLB);
                        resultApprox.poll();
                        resultApprox.add(in);
                        refineCount++;
                    }
                }
            }

            System.out.print("firstFlterCount = "+(datasetID.size()- firstFlterResult.size())+"; ");
            System.out.print("secondfilterCount =" + filterCount+"; ");
            System.out.print("refineCount = "+ refineCount+"; ");

            long endTime3 = System.currentTimeMillis(); //
            System.out.print("create_tree = "+(endTime2 - endTime1)+"; ");
            System.out.print("first filter time = "+(FirstFilterTime - endTime2)+"; ");
            System.out.print("second filter time = "+(endTime2 - endTime1)+"; ");
            System.out.println("total_time = "+(endTime3 - endTime1)+"; ");
            time[i] = endTime3 - endTime1;
        }
        long totalTime=0;
        for (int i = 0; i<time.length; i++){
            totalTime = totalTime+time[i];
        }
        double averageTime = totalTime/total_query_count;
        System.out.println("average_time =  "+averageTime);

    }

    public static void oneLevelIndex(ArrayList<Integer> query, String DEBUG_LEVEL) throws IOException,CloneNotSupportedException, FileNotFoundException {
        System.out.println("DEBUG_LEVEL = "+ DEBUG_LEVEL +"; topk = "+ topk+"; resolution = "+resolution+"; totalNumberOfLine = "+totalNumberOfLine);
        long[] time = new long[query.size()];
        for (int i = 0; i<query.size(); i++) {
            System.out.print( "queryDataRowNumber  =  " +query.get(i)+" ; ");
            queryDataRowNumber = query.get(i);
            generateQuery(queryDataRowNumber);

            ArrayList<Integer> datasetID = getAllData();
            long startTime = System.currentTimeMillis();
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
        double averageTime = totalTime/total_query_count;
        System.out.println("average_time =  "+averageTime);

    }


    public static ArrayList<Integer> getAllData() throws IOException,CloneNotSupportedException {
        ArrayList<double[]> l = new ArrayList<>();
        DoubleArrayList ub = new DoubleArrayList();
        ArrayList<String[]> datasetList_after_pooling = getPooling();
        ArrayList<Integer> his = new ArrayList(); //his coresponding to the all histogram_name
        ArrayList<Integer> datasetID = new ArrayList<>();
        int numberOfLine = 0;
        while (numberOfLine< totalNumberOfLine && numberOfLine<histogramList.size()) {
            ArrayList<double[]> allPoints = new ArrayList<>();
            HashMap<Long, Double> map1 = histogramList.get(numberOfLine);
            for (long key : map1.keySet()) {
                long[] coord = readZcurve.resolve(key);
                double[] d = new double[3];
                d[0] = Double.parseDouble(String.valueOf(coord[0]));
                d[1] = Double.parseDouble(String.valueOf(coord[1]));
                d[2] = map1.get(key);
                allPoints.add(d);
            }
            //sampleData
            String[] buf = datasetList_after_pooling.get(numberOfLine);
            allHistogram.put(numberOfLine, allPoints);
            datasetID.add(numberOfLine);
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
        int countOfRow = numberOfLine;// before -1 is subtract the query data, here needn't -1;
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

    public static ArrayList<String[]> getPooling() throws CloneNotSupportedException, IOException {
        pooling p = new pooling();
        ArrayList<String[]> dataSetList_after_pooling = new ArrayList<String[]>();
        for (int id = 0; id < histogramList.size(); id++) {
            HashMap<Long, Double> map1 = histogramList.get(id);
            signature_t s1 = getSignature(map1);
            signature_t s1_pooling = p.poolingOperature(s1, 1);
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


    public static ArrayList<HashMap<Long, Double>> generateHistogram() throws CloneNotSupportedException{
        ArrayList<HashMap<Long, Double>> histogram = new ArrayList<>();
        if (decreaseOfResulution == 0){
            for (int id: dataSetMap.keySet()){
                HashMap<Long, Double> map1 = dataSetMap.get(id);
                histogram.add(map1);
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
            }
        }else {
            System.out.println("decreaseOfResulution < 0");
        }
        return histogram;
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

//        HashMap<Integer, HashMap<Long,Double>> resultWithRespoID = new HashMap<>();
//        for (Integer i : result.keySet() ){
//            HashMap<Long,Double> hm = new HashMap<>();
//            for (Long j : result.get(i).keySet()){
//                System.out.println("key: " + j + " value: " + result.get(i).get(j));
//            }
//        }
        return result;
    }

    public static int countLines(String filename) throws IOException {
        LineNumberReader reader  = new LineNumberReader(new FileReader(filename));
        int cnt = 0;
        String lineRead = "";
        while ((lineRead = reader.readLine()) != null) {}

        cnt = reader.getLineNumber();
        reader.close();
        return cnt;
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
        System.out.println("refineCount = " + refineCount+";");

//        long endTime2 = System.currentTimeMillis(); //
//        System.out.println("Exact EMD time==== " + (endTime2 - startTime) + "ms");
        ArrayList<Double> topK_EMD = new ArrayList<>();
        ArrayList<String> resultString = new ArrayList<>();
        ArrayList<Integer> topK_ResultID = new ArrayList<>();
        while (!result.isEmpty()){
            relaxIndexNode r = result.poll();
            String s = "getSignatureID: "+r.resultId + ", Value: " + r.lb;
            resultString.add(s);
            topK_ResultID.add(r.resultId);
            topK_EMD.add(r.lb);
        }

//        for (int j = 0; j<topK_ResultID.size(); j++){
//            System.out.println(j+" , "+topK_ResultID.get(j)+" , "+topK_EMD.get(j));
//        }
        return topK_ResultID; //reverse
    }

    public static ArrayList getExactEMD(PriorityQueue<relaxIndexNode> secondFilterResult, int topk) throws CloneNotSupportedException{
        //Due to aIntegers.add(id+1) in traverseConvert2, Thus the rowNumber+1 is stored in resultID
        long startTime = System.currentTimeMillis();
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

//        long endTime2 = System.currentTimeMillis(); //
//        System.out.println("Exact EMD time==== " + (endTime2 - startTime) + "ms");
        System.out.println("refineCount = " + refineCount+";");
        ArrayList<String> resultString = new ArrayList<>();
        ArrayList<Integer> topK_ResultID = new ArrayList<>();
        ArrayList<Double> topK_EMD = new ArrayList<>();
        while (!result.isEmpty()){
            relaxIndexNode r = result.poll();
//            String s = "getSignatureID: "+r.resultId+",  Key: " + histogram_name[r.resultId-1] + ", Value: " + r.lb;
            String s = "getSignatureID: "+r.resultId + ", Value: " + r.lb;
            resultString.add(s);
            topK_ResultID.add(r.resultId);
            topK_EMD.add(r.lb);

        }
//        for (int j = 0; j<topK_ResultID.size(); j++){
//            System.out.println(j+" , "+topK_ResultID.get(j)+" , "+topK_EMD.get(j));
//        }

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
    public static signature_t getSignature(HashMap<Long,Double> map){
        int n = map.size();
        feature_t[] Features = new feature_t[n];
        double []Weights = new double[n];
        long []Coordinates;
        double unit = 360/Math.pow(2,resolution);
        int i=0;
        for(long key:map.keySet()){//??map
            Coordinates = readZcurve.resolve(key);//?zcode?????????
            Features[i] = new feature_t(Coordinates[0],Coordinates[1]);
            Weights[i] = map.get(key);
            i++;
        }
        signature_t s = new signature_t(n,Features,Weights);
        return s;
    }
    public static indexNode createBallTree() throws IOException,CloneNotSupportedException {
//        long startTime = System.currentTimeMillis();
        int leafThreshold = leaf_Threshold;
        int maxDepth = max_Depth; // 2^22 = 104 8576
        long startTime = System.currentTimeMillis();
//        sampleData(); sampleData ?????getAllData()????????
        long endTime1 = System.currentTimeMillis();
        indexNode in = ball_tree.create(ubMove, iterMatrix, leafThreshold, maxDepth, dimension);
        long endTime2 = System.currentTimeMillis(); // current time
//        System.out.println("ballTree.getTotalCoveredPoints()==" + in.getTotalCoveredPoints());
        //       System.out.println("getPoolingTime = "+(endTime1-startTime)+"ms,    create_balltree= " + (endTime2 - endTime1) + "ms");
        return in;
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
        }
        return resultID;
    }

    public static double distance(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return Math.sqrt(d);
    }

}
