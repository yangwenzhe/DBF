package ballTree;

import au.edu.rmit.trajectory.clustering.kmeans.indexNode;
import es.saulvargas.balltrees.BallTreeMatrix;
import es.saulvargas.balltrees.BinaryTree;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Random;

public class ball_tree extends BinaryTree {
    private static final Random random = new Random();

    public ball_tree(NodeBall root){super(root);}
    public BallTreeMatrix.Ball getRoot(){
        return (BallTreeMatrix.Ball) super.getRoot();
    }
    public static indexNode create(double[] ubMove,double[][] iterMatrix, int leafThreshold, int maxDepth){
        int[] rows = new int[iterMatrix.length];
        for (int row = 0; row < iterMatrix.length; row++){
            rows[row] = row;
        }
        BallTreeMatrix.Ball root = new BallTreeMatrix.Ball(ubMove, rows, iterMatrix);
        indexNode rootKmeans = new indexNode(iterMatrix[0].length);
        int depth = 0;
        if (rows.length > leafThreshold && depth<maxDepth){
            createChildren(root, leafThreshold, depth+1, maxDepth);
        }
        //???
        root.traverseConvert2(rootKmeans, iterMatrix[0].length);
        return rootKmeans;
    }

    public static indexNode create(double[] ubMove,double[][] itemMatrix, int leafThreshold, int maxDepth,  int dimension) {
        int[] rows = new int[itemMatrix.length];
        for (int row = 0; row < itemMatrix.length; row++) {
            rows[row] = row;
        }
//        System.out.println("create function rows ===" + rows.length);
        BallTreeMatrix.Ball root = new BallTreeMatrix.Ball(ubMove, rows, itemMatrix);
        indexNode rootKmeans = new indexNode(dimension);

        int depth = 0;
        if (rows.length > leafThreshold && depth < maxDepth) {
            createChildren(root, leafThreshold, depth + 1, maxDepth);
        }
//        System.out.println("root.getRowsID().length==="+root.getRowsID().length);
        root.traverseConvert2(rootKmeans, dimension);
        return rootKmeans;
    }

    private static void createChildren(BallTreeMatrix.Ball parent, int leafThreshold, int depth, int maxDepth) {
        IntArrayList leftRows = new IntArrayList();
        IntArrayList rightRows = new IntArrayList();

        splitItems(parent.getRows(), parent.getItemMatrix(), leftRows, rightRows);
//        System.out.println("leftRows=="+leftRows.size());
//        System.out.println("rightRows=="+rightRows.size());
        parent.clearRows();
        BallTreeMatrix.Ball leftChild = new BallTreeMatrix.Ball(parent.getUbMove(),leftRows.toIntArray(), parent.getItemMatrix());
        BallTreeMatrix.Ball rightChild = new BallTreeMatrix.Ball(parent.getUbMove(),rightRows.toIntArray(), parent.getItemMatrix());

        parent.setLeftChild(leftChild);
        if (leftChild.getRows().length > leafThreshold && depth < maxDepth && rightChild.getRows().length > 0) {
            createChildren(leftChild, leafThreshold, depth + 1, maxDepth);
        }

        parent.setRightChild(rightChild);
        if (rightChild.getRows().length > leafThreshold) {
            createChildren(rightChild, leafThreshold, depth + 1, maxDepth);
        }

    }

    protected static void splitItems(int[] rows, double[][] itemMatrix, IntArrayList leftRows, IntArrayList rightRows) {
        // pick random element
//        try {
//            FileWriter fw = new FileWriter("D:\\GitHub\\Argov\\data\\1.txt",true);
//
//            fw.write("splitItems fuction rows length ==="+rows.length);
//            fw.write("\n");

            double[] x = itemMatrix[rows[new Random().nextInt(rows.length)]]; //random.nextInt(rows.length)
            // select furthest point A to x
            double[] A = x;
            double dist1 = 0;
            for (int row : rows) {
                double[] y = itemMatrix[row];
                double dist2 = distance2(x, y);
                if (dist2 > dist1) {
                    A = y;
                    dist1 = dist2;
                }
            }
            // select furthest point B to A
            double[] B = A;
            dist1 = 0;
            for (int row : rows) {
                double[] y = itemMatrix[row];
                double dist2 = distance2(A, y);
                if (dist2 > dist1) {
                    B = y;
                    dist1 = dist2;
                }
            }
            // split data according to A and B proximity
            for (int row : rows) {
                double[] y = itemMatrix[row];
                double distA = distance2(A, y);
                double distB = distance2(B, y);
                if (distA <= distB) {
                    leftRows.add(row);
                } else {
                    rightRows.add(row);
                }
            }
//            if (rightRows.size() == 0){
//                fw.write("leftrows.length === " + leftRows.size()+ "    ");
//                fw.write("rightrows.length === " + rightRows.size());
//                fw.write("\n");
//                for (int id: leftRows){
//                    fw.write(itemMatrix[id][0]+", "+itemMatrix[id][1]+"   ");
//                }
//                fw.write("\n");
//                for (int id: rightRows){
//                    fw.write(itemMatrix[id][0]+", "+itemMatrix[id][1]+"   ");
//                }
//                fw.write("\n");
//            }
//            fw.write("\n");
//
//            fw.close();
//        }catch (IOException e){
//            System.out.println(e);
//        }

    }
    //
    public static double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
                d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return d;
    }
    //????
    public static double distance(double[] x, double[] y) {
        return Math.sqrt(distance2(x, y));
    }

}
