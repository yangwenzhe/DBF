package statisticOfDataset;

import entity.feature_t;
import entity.signature_t;
import util.readZcurve;
import util.storeZcurve;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class preProcessing {
    public static double range = 360;
    public static int original_resolution; //unit=range/(2^resolution)
    public static int after_resolution;
    public static int decreaseOfResulution ;
    public static String zcurveFilePath;//"D:\\GitHub\\ZorderCurve\\src\\data\\gpx-Test.ser";//存储zcurve文件的文件路径 D:\GitHub\ZorderCurve\src\data\gpx-trackable-hashmap.ser

    public static HashMap<Integer,HashMap<Long,Double>> dataSetMap;

    public static ArrayList<HashMap<Long, Double>> generateHistogram(){
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
                    long zcode = storeZcurve.combine(transferCoordinates[0],transferCoordinates[1],after_resolution);
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

    public static void main(String[]args){

//        original_resolution =Integer.parseInt(System.getProperty("original_resolution"));
//        after_resolution = Integer.parseInt(System.getProperty("after_resolution"));
//        zcurveFilePath = System.getProperty("zcurveFilePath");//"D:\\GitHub\\ZorderCurve\\src\\data\\gpx-Test10.ser";  // "/home/wzyang/java/gpx/gpx-Test20.ser";
//        decreaseOfResulution = original_resolution - after_resolution;
        original_resolution =16;
        zcurveFilePath = "/home/gr/wzyang/java/public/public-16.ser";//"D:\\GitHub\\ZorderCurve\\src\\data\\gpx-Test10.ser";  //

        dataSetMap = deSerializationZcurve(zcurveFilePath);
        int [] after_resolution_list = new int[]{11};//15,14,13,12,
        for (int i = 0; i<after_resolution_list.length; i++){
            after_resolution = after_resolution_list[i];
            decreaseOfResulution = original_resolution - after_resolution;


            long startTime = System.currentTimeMillis();
            generateHistogram();
            long endTime = System.currentTimeMillis();
            System.out.println("orig_resolution = " + original_resolution +" , " + "after_resolution = " + after_resolution +" , " +(endTime - startTime)+"ms" );

        }

 }

    public static signature_t getPoolingSignature(HashMap<Long,Double> map){
        long []Coordinates;
        HashMap<Long, Double> hm = new HashMap<Long, Double>();
        for (long key: map.keySet()){
            Coordinates = resolve(key);
            double weight = map.get(key);
            long[] transferCoordinates = new long[2];
            int t =(int)Math.pow(2,decreaseOfResulution);
            transferCoordinates[0] = Coordinates[0]/t ;
            transferCoordinates[1] = Coordinates[1]/t ;
            long zcode = storeZcurve.combine(transferCoordinates[0],transferCoordinates[1],after_resolution);
            if (hm.get(zcode) == null){
                hm.put(zcode, weight);
            }else{
                double temp = hm.get(zcode);
                hm.put(zcode,weight+temp);
            }
        }
        int n = hm.size();
        feature_t[] Features = new feature_t[n];
        double[] Weights = new double[n];
        int i=0;
        long []after_Coordinates;
        for (long key : hm.keySet()){
            after_Coordinates = resolve(key);
            Features[i] = new feature_t(after_Coordinates[0], after_Coordinates[1]);
            Weights[i] = hm.get(key);
            i++;
        }
        signature_t s = new signature_t(n, Features, Weights );
        return s;
    }

    public static signature_t getSignature(HashMap<Long,Double> map){
        int n = map.size();
        feature_t[] Features = new feature_t[n];
        double []Weights = new double[n];
        long []Coordinates;
        double unit = range/Math.pow(2,original_resolution);
        int i=0;
        for(long key:map.keySet()){
            Coordinates = resolve(key);
            Features[i] = new feature_t(Coordinates[0],Coordinates[1]);
            Weights[i] = map.get(key);
            i++;
        }
        signature_t s = new signature_t(n,Features,Weights);
        return s;
    }

    public static long[] resolve(long code){
        long[] Coordinates = new long[2];
        //int length = (int)Math.pow(2,resolution);

        StringBuilder a = new StringBuilder();
        StringBuilder b = new StringBuilder();

        String str = Long.toBinaryString(code);
        int length = str.length();


        for(int i=0;i<length;i++){
            if(i%2==0)
                a.append(str.charAt(i));
            else
                b.append(str.charAt(i));
        }

        Coordinates[0] = Integer.parseInt(a.toString(),2);
        Coordinates[1] = Integer.parseInt(b.toString(),2);
        return Coordinates;
    }

    /*
     * load the zcurve into memory
     */
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
