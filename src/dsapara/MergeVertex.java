/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jiangwei
 *
 * This is most update correlation function for tweets and followee, and
 * follower
 */
public class MergeVertex extends PrintFunction implements Runnable {

    /**
     * @param args the command line arguments
     */
    //  public String uidFProperty = "userIDsfollowquad.txt";
    //   public String uidTProperty = "userIDsTweetquad.txt";
    //  public String tweetDegreeCounts = "tweetDegreeCountshalf.txt";
    public String calTweetF = "0.2calfidDegree.txt";
    // public String calTweetF = "caltweetfull.txt";
//    public String calUserF = "calfollowerfull.txt";

    public String calUserF = "0.2caluidDegree.txt";
    // public int calTweetSize = 2;
    public int calUserSize = 1;
    //   public String corrOriginal = "quaduser.txt";
    public String corrOriginal = "keyDegree.txt";

// public int followSecondPoint = 65;
    //  public int tweetSecondPoint = 459;
    public double domainRatio = 1;
    //  public int closeRange = 2000;
    public String printF = "0.2degree.txt";
    //   public String printF = "usersfull.txt";
    public double stime = 0.2;
    public int scaledVertexSize = 0;
    public String userMap = "0.2userMap.txt"; //corresponding to the top 0.1% nodes corresponding.
    boolean stop = false;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree;

    //   public int sourceAfter = 26615;
    //The approach of the correlation is based on the original degree distribution.
    // 1. sort the degree in descending order. And settle the value from largest to the smallest
    // 2. Find the closest if the value is not found
    public MergeVertex() {
    }

    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> result;
    ArrayList<ComKey> key;
    ArrayList<HashMap<Integer, Integer>> arrspara;
    HashMap<ArrayList<Integer>, Integer> value;

    MergeVertex(HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> result, ArrayList<ComKey> key, ArrayList<HashMap<Integer, Integer>> arrs, HashMap<ArrayList<Integer>, Integer> value) {
        this.key = key;
        this.arrspara = arrs;
        this.result = result;
        this.value = value;
    }

    double rationP = 0.005;
    HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>> mapped = new HashMap<>();

    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> corrMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution, HashMap<ArrayList<String>, HashMap<Integer, Integer>> downsizedCounts) {
        // HashMap<ArrayList<ArrayList<Integer>>, Integer> correlatedOriginal = loadCorrAll(corrOriginal);
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : mergedDistribution.entrySet()) {
            ArrayList<HashMap<Integer, Integer>> arrs = new ArrayList<>();
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> arr = new ArrayList<>();
                arr.add(entry.getKey().get(0));
                arr.add(entry.getKey().get(i));
                arrs.add(downsizedCounts.get(arr));
            }
            //      System.out.println("start");
            //      System.out.println(entry.getKey());
            HashMap<ArrayList<Integer>, Integer> correlated = produceCorr(entry.getValue(), arrs);
            result.put(entry.getKey(), correlated);
            //   System.out.println("end");
        }
        return result;

    }

    int summ = 0;
    int round = 0;

    private HashMap<ArrayList<Integer>, Integer> produceCorr(HashMap<ArrayList<Integer>, Integer> value, ArrayList<HashMap<Integer, Integer>> arrs) {
        int preV = 0;
        summ = 0;
        int num = thirdRound(arrs);
        HashMap<ArrayList<Integer>, Integer> result = mapping(value, arrs);
        round++;

         num = thirdRound(arrs);
        boolean flag = false;

        while (!arrs.get(0).keySet().isEmpty()) {
            System.out.println(num + "   " + key.get(0).toString());

            HashMap<ArrayList<Integer>, Integer> result1 = mapping(value, arrs);
            for (Entry<ArrayList<Integer>, Integer> entry : result1.entrySet()) {
                if (!result.containsKey(entry.getKey())) {
                    result.put(entry.getKey(), 0);
                }
                result.put(entry.getKey(), result.get(entry.getKey()) + entry.getValue());
            }
            preV = num;
            num = thirdRound(arrs);
        }

        int count = 0;
        for (Entry<ArrayList<Integer>, Integer> entry : value.entrySet()) {
            boolean flags = true;
            for (int i : entry.getKey()) {
                if (i <= 0) {
                    flags = false;
                }
            }
            if (flags) {
                count += entry.getValue();
            }
        }

        int count1 = 0;
        for (Entry<ArrayList<Integer>, Integer> entry : result.entrySet()) {
            boolean flags = true;
            for (int i : entry.getKey()) {
                if (i <= 0) {
                    flags = false;
                }
            }
            if (flags) {
                count1 += entry.getValue();
            }
        }
        this.mappedBestJointDegree.put(key, mapped);
        System.out.println(count + "   " + count1);
        return result;
    }
    boolean empty = false;
    

    private HashMap<ArrayList<Integer>, Integer> mapping(HashMap<ArrayList<Integer>, Integer> originalDis, ArrayList<HashMap<Integer, Integer>> arrs) {
        int size = arrs.size();
        for (int i = 0; i < size; i++) {
            ArrayList<Integer> temp = new ArrayList<Integer>(arrs.get(i).keySet());
            for (int entry : temp) {
                if (arrs.get(i).get(entry) == 0) {
                    arrs.get(i).remove(entry);
                }
            }
        }
        HashMap<ArrayList<Integer>, Integer> result = new HashMap<>();
        int value = 0;
        Sort so = new Sort();
        List<Entry<ArrayList<Integer>, Integer>> sorted = so.sortOnKeySum1(originalDis);
        for (int i = 0; i < sorted.size() && !arrs.get(0).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<Integer>, Integer> entry = sorted.get(i);
            ArrayList<Integer> calDegs = new ArrayList<>();

            // find the closest matching joint degree
            if (round == 0) {
                boolean ntfound = checkFound(entry, arrs);
                if (empty) {
                    break;
                }
                if (ntfound) {
                    continue;
                }

                calDegs = entry.getKey();

            } else {
                calDegs = getDegrees(entry, arrs);
                if (empty) {
                    break;
                }
            }

            value = calValue(entry);

            for (int j = 0; j < calDegs.size(); j++) {
                value = Math.min(value, arrs.get(j).get(calDegs.get(j)));
            }

            if (value < 0) {
                value = 0;
                continue;
            }

            if (value > 0) {
                settleMapping(entry, calDegs);
            }
            summ += value;

            for (int j = 0; j < calDegs.size(); j++) {
                int count = arrs.get(j).get(calDegs.get(j));
                arrs.get(j).put(calDegs.get(j), count - value);
            }

            if (!result.containsKey(calDegs)) {
                result.put(calDegs, 0);
            }
            //      System.out.println(entry.getKey()+"+++++++"+calDegs);
            result.put(calDegs, value + result.get(calDegs));

            for (int j = 0; j < arrs.size(); j++) {
                if (arrs.get(j).containsKey(calDegs.get(j)) && arrs.get(j).get(calDegs.get(j)) == 0) {
                    arrs.get(j).remove(calDegs.get(j));
                }
            }

        }
        return result;
    }

    private int thirdRound(ArrayList<HashMap<Integer, Integer>> arrs) {
        int min = Integer.MAX_VALUE;
        
        for (HashMap<Integer, Integer> map : arrs) {
            int sum = 0;
            for (int v : map.values()) {
                sum += v;
            }
            min = Math.min(sum, min);
        }
        return min;
    }

    private int findClosest(Integer deg, Set<Integer> keySet) {
        if (keySet.size() == 0) {
            return 0;
        }
        if (keySet.contains(deg)) {
            return deg;
        }
        List<Integer> arr = new ArrayList<>();
        for (int i : keySet) {
            arr.add(i);
        }
        int min = Integer.MAX_VALUE;
        int result = arr.get(0);
        //    System.out.println(arr);
        for (int i = 0; i < arr.size(); i++) {
            if (Math.abs(arr.get(i) - deg) < min) {
                min = Math.abs(arr.get(i) - deg);
                result = arr.get(i);
            }
        }
        return result;
    }

    @Override
    public void run() {

        result.put(key, produceCorr(value, arrspara));
        stop = true;
    }

    private boolean checkFound(Entry<ArrayList<Integer>, Integer> entry, ArrayList<HashMap<Integer, Integer>> arrs) {

        for (int j = 0; j < entry.getKey().size(); j++) {
            if (arrs.get(j).keySet().size() == 0) {
                empty = true;
                return true;

            }
            if (!arrs.get(j).containsKey(entry.getKey().get(j))) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<Integer> getDegrees(Entry<ArrayList<Integer>, Integer> entry, ArrayList<HashMap<Integer, Integer>> arrs) {
        ArrayList<Integer> calDegs = new ArrayList<>();
        for (int j = 0; j < entry.getKey().size(); j++) {
            if (arrs.get(j).keySet().size() == 0) {
                empty = true;
                break;
            }
            int closeDeg = findClosest(entry.getKey().get(j), arrs.get(j).keySet());

            calDegs.add(closeDeg);
        }
        return calDegs;
    }

    private int calValue(Entry<ArrayList<Integer>, Integer> entry) {
        int value = (int) Math.floor(entry.getValue() * stime);
        double diff = entry.getValue() * stime - value;
        double kl = Math.random();
        if (kl < diff) {
            value++;
        }
        return value;
    }

    private void settleMapping(Entry<ArrayList<Integer>, Integer> entry, ArrayList<Integer> calDegs) {
        if (!mapped.containsKey(entry.getKey())) {
            mapped.put(entry.getKey(), new ArrayList<ArrayList<Integer>>());
        }
        if (!mapped.get(entry.getKey()).contains(calDegs)) {
            mapped.get(entry.getKey()).add(calDegs);
        }
    }

   
}
