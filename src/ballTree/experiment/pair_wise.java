package ballTree.experiment;

import entity.feature_t;
import entity.signature_t;
import util.emd_class;
import util.readZcurve;
import util.relax_EMD;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class pair_wise {

    public static String zcurveFilePath ="/home/gr/wzyang/java/Argov/ArgovHashmap.ser"; // "D:\\GitHub\\Argov\\data\\Argov\\ArgovHashmap.ser";//
    public static HashMap<Integer, HashMap<Long,Double>> dataSetMap = deSerializationZcurve(zcurveFilePath);

    public static void runtimeCompare() throws CloneNotSupportedException, FileNotFoundException{
        //        PrintStream ps = new PrintStream("D:\\GitHub\\Argov\\data\\Argov\\ICT_IMSIG.txt"); ////"/home/gr/wzyang/java/pairWise-removeOne-10000.txt"
//        System.setOut(ps);
        ArrayList<HashMap<Long, Double>> histogramList = generateHistogram();
        int queryDataRowNumber = 1;
        signature_t query = getQuerySignature(histogramList,queryDataRowNumber);



        ArrayList<signature_t> dataList = new ArrayList<>();
        for (int i =2; i < 200002; i++) {
            signature_t data = getQuerySignature(histogramList,i);
            dataList.add(data);
        }
        relax_EMD re = new relax_EMD();

        long tighter_ICTtime1=0;
        long tighter_ICTtime2=0;
        for (int i = 2; i < 200002; i++) {
            signature_t data = dataList.get(i - 2);
            if (i == 2) {
                tighter_ICTtime1 = System.currentTimeMillis();
            }
            double tighter_ICT = re.tighter_ICT(data, query);

            if ((i - 1) % 10000 == 0) {
                tighter_ICTtime2 = System.currentTimeMillis();
                System.out.println("tighter_ICT = " + (tighter_ICTtime2 - tighter_ICTtime1));
                tighter_ICTtime1 = tighter_ICTtime2;
            }
        }

        long ICTtime1=0;
        long ICTtime2=0;
        for (int i = 2; i < 200002; i++) {
            signature_t data = dataList.get(i - 2);
            if (i == 2) {
                ICTtime1 = System.currentTimeMillis();
            }
            double ICT = re.ICT(data, query);

            if ((i - 1) % 10000 == 0) {
                ICTtime2 = System.currentTimeMillis();
                System.out.println("ICT = " + (ICTtime2 - ICTtime1));
                ICTtime1 = ICTtime2;
            }
        }

        long IM_SIGtime1=0;
        long IM_SIGtime2=0;
        for (int i = 2; i < 200002; i++) {
            signature_t data = dataList.get(i - 2);
            if (i == 2) {
                IM_SIGtime1 = System.currentTimeMillis();
            }
            double IM_SIG = re.IM_SIG_star(data, query);

            if ((i - 1) % 10000 == 0) {
                IM_SIGtime2 = System.currentTimeMillis();
                System.out.println("IM_SIG = " + (IM_SIGtime2 - IM_SIGtime1));
                IM_SIGtime1 = IM_SIGtime2;
            }
        }

        long removeTime1=0;
        long removeTime2=0;
        for (int i = 2; i < 200002; i++) {
            signature_t data = dataList.get(i - 2);
            if (i == 2) {
                removeTime1 = System.currentTimeMillis();
            }
            double removeOneConstraint = re.removeOneConstraintEMD(data,query);

            if ((i - 1) % 10000 == 0) {
                removeTime2 = System.currentTimeMillis();
                System.out.println("remove one constraint = " + (removeTime2 - removeTime1));
                removeTime1 = removeTime2;
            }
        }
    }


    public static void main(String [] args) throws CloneNotSupportedException, FileNotFoundException {
        String[] fourDataSetName = new String[]{"Argov", "trackable","identifiable","public"};//
        for (int ID=0; ID<fourDataSetName.length; ID++){
            String datasetName = fourDataSetName[ID];
            String serverName =  "/home/gr/wzyang/java/"; //"D:/GitHub/ZorderCurve/data/";//
            int resolution = 13; //Note******************
            System.out.println("datasetName = "+datasetName+", resolution = "+resolution);
            switch (datasetName){
                case "Argov": resolution = 7;
                    break;
            }
            zcurveFilePath = serverName+datasetName+"/"+datasetName+"-"+resolution+".ser";//"D:\\GitHub\\ZorderCurve\\data\\identifiable\\identifiable-"+resolution+".ser";//
            dataSetMap = deSerializationZcurve(zcurveFilePath);
            ArrayList<HashMap<Long, Double>> histogramList = generateHistogram();
            int queryDataRowNumber = 1;
            signature_t query = getQuerySignature(histogramList,queryDataRowNumber);

            double error_tighter_ICT = 0.0;
            double error_ICT = 0.0;
            double error_IM_SIG_star = 0.0;
            double error_RWMD = 0.0;

            long tighterTime = 0;long ICTTime = 0;long IM_SIG_starTime = 0; long RWMDTime = 0;

            int count = 0;
            for (int i = 2; i < 1001; i++){
                signature_t data = getQuerySignature(histogramList,i);
                relax_EMD re = new relax_EMD();
                emd_class hh = new emd_class();


                double EMD = 0;// hh.emd(data, query, null);
                count++;

                long time2 = System.currentTimeMillis();
                double tighter_ICT = re.tighter_ICT(data, query);
                long time3 = System.currentTimeMillis();
                tighterTime += (time3-time2);


                double ICT = re.ICT(data, query);
                long time4 = System.currentTimeMillis();
                ICTTime += (time4-time3);


                double IM_SIG_star = re.IM_SIG_star(data, query);
                long time5 = System.currentTimeMillis();
                IM_SIG_starTime += (time5-time4);


                double RWMD = re.removeOneConstraintEMD(data,query);
                long time6 = System.currentTimeMillis();
                RWMDTime += (time6-time5);

                error_tighter_ICT += (EMD - tighter_ICT)/EMD;
                error_ICT+=(EMD - ICT)/EMD;
                error_IM_SIG_star +=(EMD-IM_SIG_star)/EMD;
                error_RWMD +=(EMD-RWMD)/EMD;
                System.out.print(i+", ");
//            System.out.print("EMD = "+EMD+" ; ");
//            System.out.print("tighter_ICT = "+tighter_ICT+" ; ");
//            System.out.print("ICT = "+ICT+" ; ");
//            System.out.println("IM_SIG_star = "+IM_SIG_star+" ; ");
//            System.out.println("RWMD = "+RWMD+"; ");


//            System.out.println(EMD+","+tighter_ICT+","+tighter_sort_ICT+","+ICT+","+IM_SIG+","+centroid_lower_bound);
            }
            error_tighter_ICT = error_tighter_ICT/count;
            error_ICT = error_ICT/count;
            error_IM_SIG_star = error_IM_SIG_star/count;
            error_RWMD = error_RWMD/count;

            System.out.println("totalTime =  "+ tighterTime +"," +ICTTime+","+IM_SIG_starTime+","+RWMDTime);
            System.out.println("averageError = "+error_tighter_ICT+","+error_ICT+","+error_IM_SIG_star+","+error_RWMD);
        }
    }

    public static ArrayList<HashMap<Long, Double>> generateHistogram() throws CloneNotSupportedException{
        ArrayList<HashMap<Long, Double>> histogram = new ArrayList<>();
        for (int id: dataSetMap.keySet()){
            HashMap<Long, Double> map1 = dataSetMap.get(id);
            histogram.add(map1);
        }
        return histogram;
    }


    public static signature_t getQuerySignature(ArrayList<HashMap<Long, Double>> histogramList, int pDataRowNumber){
        HashMap<Long, Double> map1 = histogramList.get(pDataRowNumber);
        ArrayList<double[]> allPoints = new ArrayList<>();
        for (long key: map1.keySet()){
            long[] coord = readZcurve.resolve(key);
            double [] d = new double[3];
            d[0] = Double.parseDouble(String.valueOf(coord[0]));
            d[1] = Double.parseDouble(String.valueOf(coord[1]));
            d[2] = map1.get(key);
            allPoints.add(d);
        }
        int n = allPoints.size();
        feature_t[] features = new feature_t[n];
        double[] weights = new double[n];
        for (int i = 0; i < n; i++) {
            features[i] = new feature_t(allPoints.get(i)[0], allPoints.get(i)[1]);
            weights[i] = allPoints.get(i)[2];
        }
        signature_t s = new signature_t(n, features, weights);
        return s;
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

}
