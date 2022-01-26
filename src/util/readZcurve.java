package util;

import entity.feature_t;
import entity.signature_t;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class readZcurve {
    public static double range = 360;
    public static int resolution; //unit=range/(2^resolution)
    public static String zcurveFilePath;//"D:\\GitHub\\ZorderCurve\\src\\data\\gpx.ser";//存储zcurve文件的文件路径 D:\GitHub\ZorderCurve\src\data\gpx-trackable-hashmap.ser
    public static String histogramPath;

    public static HashMap<Integer,HashMap<Long,Double>> dataSetMap;
    public static void main(String[]args) throws FileNotFoundException{

        resolution =12; //Integer.parseInt(System.getProperty("resolution"));//
        zcurveFilePath = "D:\\GitHub\\ZorderCurve\\data\\public\\public-12.ser";//System.getProperty("zcurveFilePath");
        dataSetMap = deSerializationZcurve(zcurveFilePath);
        System.out.println("dataSetMap.size() = "+dataSetMap.size());
        PrintStream ps = new PrintStream("D:\\GitHub\\ZorderCurve\\data\\public\\12"+"signature.txt");//data\\"+DEBUG_LEVEL+"__"+totalNumberOfLine+".txt");
        System.setOut(ps);
        ArrayList<HashMap<Long, Double>> histogram = new ArrayList<>();
        for (int id: dataSetMap.keySet()){
            HashMap<Long, Double> map1 = dataSetMap.get(id);
            signature_t s1 = getSignature(map1);
            System.out.println(s1.n);
            histogram.add(map1);
        }
        ps.close();

    }

    public static signature_t getSignature(HashMap<Long,Double> map){
        int n = map.size();
        feature_t[] Features = new feature_t[n];
        double []Weights = new double[n];
        long []Coordinates;
        double unit = 1.0;
        int i=0;
        for(long key:map.keySet()){
            Coordinates = resolve(key);
            Features[i] = new feature_t(Coordinates[0]*unit,Coordinates[1]*unit);
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
        Coordinates[0] = Long.parseLong(a.toString(),2);
        Coordinates[1] = Long.parseLong(b.toString(),2);
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
