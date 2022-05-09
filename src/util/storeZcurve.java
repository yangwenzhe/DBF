package util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class storeZcurve {
    public static double minX = -90; //-120;
    public static double minY = -180; //500;
    public static double range = 360;
    public static int resolution; //unit=range/(2^resolution)
    public static String dataPath;//"D:\\gpx-planet\\";
    //public 546194; trackable 66380; identifiable 235482;
    public static int maxFileName;
    public static String zcurveFilePath;// "D:\\IdeaProjects\\ZorderCurve\\src\\data\\gpx-"+ "trackable-"+"Test.ser";
    public static Map<Integer, double[][]> datamap = new HashMap<Integer, double[][]>();//??????
    public static int sumOfWeights = 1;


    public static void main(String[]args){
        //public 546194; trackable 66380; identifiable 235482;
        int [] resolutionList = new int[]{11};//15,14,13,12,11
        for (int i = 0; i<resolutionList.length; i++){
            resolution = resolutionList[i];
            dataPath =  "/home/gr/wzyang/gpx-planet/public/"; //System.getProperty("dataPath");
            maxFileName = 546194;//235482;// 66380;//   //Integer.parseInt(System.getProperty("maxFileName"));
            zcurveFilePath ="/home/gr/wzyang/java/public/public-"+resolution+".ser";// "D:\\IdeaProjects\\ZorderCurve\\src\\data\\gpx-"+ "trackable-"+"Test.ser";//System.getProperty("zcurveFilePath");

            long startTime = System.currentTimeMillis();
            HashMap<Integer, HashMap<Long,Double>> testMap = storeZcurve2(minX,minY,range,2,resolution,zcurveFilePath);
            long endTime = System.currentTimeMillis();
            System.out.println("resolution = " + resolution +" , " + (endTime - startTime)+"ms" );

        }

    }
    /*
     * combine two integers to produce a new value
     */
    public static long combine(long aid, long bid, int lengtho){
        int length = lengtho;
        long[] a =new long[length];
        long[] b =new long[length];
        while(length-- >= 1){
            a[length] = aid%2;
            aid /=2;
            b[length] = bid%2;
            bid /=2;
        }
        long com[] = new long[2*lengtho];
        for(int i = 0; i<lengtho; i++){
            com[2*i]= a[i];
            com[2*i+1] = b[i];
        }
        return bitToint(com, 2*lengtho);
    }

    /*
     * generate the z-curve code
     */
    public static long bitToint(long[] a, int length){
        long sum = 0;
        for(int i=0; i<length; i++){
            sum += a[i]*Math.pow(2, length-i-1);
        }
        return sum;
    }

    public static HashMap<Integer, HashMap<Long,Double>> storeZcurve2(double minx, double miny, double range,
                                                                     int dimension,  int resolution, String zcodeFile){

        HashMap<Integer, HashMap<Long,Double>> zcodeMap = new HashMap<Integer, HashMap<Long,Double>>();
        int numberCells = (int) Math.pow(2, resolution);
        double unit = range/numberCells;
        for(int datasetid=0; datasetid<=maxFileName; datasetid++){
            HashMap<Long,Double> zcodeHashMap = new HashMap<Long, Double>();
            String Path = dataPath +datasetid+".csv";
            try{
                String record = "";
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path)));
                int lineNum = countLines(Path);
                double[][] a = new double[lineNum][];
                int i=0;
                while((record=reader.readLine())!=null){
                    String[] fields = record.split(",");
                    a[i]=new double[3];
                    a[i][0] = Double.parseDouble(fields[0]);
                    a[i][1] = Double.parseDouble(fields[1]);
                    a[i][2] = Double.parseDouble(fields[0]);
                    i++;
                }
                reader.close();
                List<double[]> list = getPositionAndVal(a);
                for(double[] d:list) {
                    long x = (long)((d[0]-minx)/unit);
                    long y = (long)((d[1]-miny)/unit);
                    long zcode = combine(x,y,resolution);
                    double val = d[2];
                    zcodeHashMap.put(zcode,val);
                }
                zcodeMap.put(datasetid, zcodeHashMap);
            }catch (IOException e) {
//            e.printStackTrace();
            }
        }
        SerializedZcurve(zcodeFile, zcodeMap);
        return zcodeMap;
    }


    public static List<double[]> getPositionAndVal(double [][]dataset){
        List<double[]> list = new ArrayList<double[]>();
        int pointNum = dataset.length;//????????
        long t = (long)Math.pow(2,resolution);
        double blockSize = range/Math.pow(2,resolution);//????
        HashMap<Long,double[]> numMap= new HashMap<Long,double[]>();//?????id??????x????y?????????????

        for(double[] p:dataset){

            double x = p[0]-minX;
            double y = p[1]-minY;
            long row = (long) (y/blockSize);
            long col = (long) (x/blockSize);
            long id = row*t+col;

            double []blockFeature = numMap.getOrDefault(id,new double[]{0,0,0});
            blockFeature[0]+=p[0]; blockFeature[1]+=p[1];blockFeature[2]+=1;
//            System.out.println("blockFeature[0] = " + blockFeature[0]+"  , "+"blockFeature[1] = " + blockFeature[1]+"  , "+"blockFeature[2] =" + blockFeature[2]+"  , ");
            numMap.put(id,blockFeature);
        }

        for(double[] b:numMap.values()){
            double x = b[0]/b[2];
            double y = b[1]/b[2];
            double val =(b[2]/pointNum)*sumOfWeights;
            list.add(new double[]{x,y,val});
        }
        return list;
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

    public static void SerializedZcurve(String file, HashMap<Integer, HashMap<Long,Double>> result) {
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(result);
            oos.close();
            fos.close();
            System.out.println("Serialized result HashMap data is saved in hashmap.ser");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}

