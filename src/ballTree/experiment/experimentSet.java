package ballTree.experiment;

import java.util.ArrayList;
import java.util.Random;

public class experimentSet {
    public static ArrayList<Integer> generateQuery(int min, int max, int query){
        ArrayList<Integer> arrayList = new ArrayList<>();
        Random random = new Random();
       while (arrayList.size() <= 100){
            int s = random.nextInt(max)%(max-min+1) + min;
            //System.out.println(s);

            if (s == query){
                continue;
            }else{
                arrayList.add(s);
            }
        }
        return arrayList;
    }
    public static void main(String[] args){
        ArrayList<Integer> a = generateQuery(1, 1000, 1);
        for (int i = 0; i<a.size(); i++){
            System.out.print(a.get(i)+",");
        }
        System.out.println();
        System.out.println(a.size());

    }



}
