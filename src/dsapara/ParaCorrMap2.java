package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author workshop
 */
public class ParaCorrMap2 implements Runnable {

    boolean eveNum = true;
    HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts;
    HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution;
    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    String ekey;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result;
    List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted;
    boolean posValue = false;
    HashSet<ArrayList<ArrayList<Integer>>> boundTrash = new HashSet<>();
    HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();
    HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();
    boolean stop = false;
    double stime = 0;
    double se = 0;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> target;
    String curTable;
    HashMap<String, ArrayList<ComKey>> referenceTable;
    int level = 0;
    int budget = 0;
    int iteration = 0;
    int indexcount = 0;
    int totalSum = 0;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree;
    HashMap<String, Boolean> uniqueNess;

    ParaCorrMap2(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result,
            HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> value,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution,
            HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {
        this.avaCounts = avaCounts;
        this.correlationFunction = value;
        this.downsizedMergedDistribution = downsizedMergedDistribution;
        this.downsizedMergedRatio = downsizedMergedRatio;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.result = result;
        this.distanceMap = distanceMap;
    }

    @Override
    public void run() {
        HashMap<ArrayList<ArrayList<Integer>>, Integer> corred = new HashMap<>();
        corred = produceCorrMap();
        result.put(this.curTable, corred);
        stop = true;

    }

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> produceCorrMap() {
        iteration = 0;
        this.boundTrash.clear();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> result = new HashMap<>();
        allBounds = new HashMap<>();
        this.totalSum = 0;
        System.out.println(avaCounts.keySet());
        mappingAllValueAesc(result);
        System.out.println(this.distanceMap.keySet());
        System.out.println("Iteration: " + this.iteration + "  " + this.curTable + " total:" + totalSum);
        while (iteration < 4 && checkAvaCoutsEmpty()) {
            level = (int) Math.pow(4, iteration);
            mappingAllValueAesc(result);
            System.out.println("Iteration: " + this.iteration + "  " + this.curTable + " total:" + totalSum);

        }
        if (checkAvaCoutsEmpty()) {
            randomRound(result);
        }
        System.out.println("RandomMatch: " + this.curTable + " total:" + totalSum);

        if (checkAvaCoutsEmpty()) {
            randSwap(result);
        }
        System.out.println("RandomSawp: " + this.curTable + " total:" + totalSum);
//        tableCorrelationDistribution
        return result;
    }

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    private ArrayList<ArrayList<Integer>> genTrans(int k, int length) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();

        if (length == 1) {
            ArrayList<Integer> temp = new ArrayList<>();
            temp.add(k);
            result.add(temp);
            return result;
        }
        if (k == 0) {
            ArrayList<Integer> temp = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                temp.add(0);
            }
            result.add(temp);
            return result;
        }

        for (int i = 0; i <= k; i++) {
            for (ArrayList<Integer> temp : genTrans(k - i, length - 1)) {
                ArrayList<Integer> local = new ArrayList<>();
                local.add(i);
                for (int v : temp) {
                    local.add(v);
                }
                result.add(local);
            }
        }
        return result;
    }

    private HashMap<Integer, ArrayList<ArrayList<Integer>>> norm1Closest(
            ArrayList<Integer> tweetDegree, ArrayList<ArrayList<Integer>> tempTweet, int k) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> ordered = new HashMap<>();
        int max = 0;

        // ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (ArrayList<Integer> sample : tempTweet) {
            int diff = 0;
            for (int i = 0; i < sample.size(); i++) {
                diff += Math.abs(sample.get(i) - tweetDegree.get(i));
            }
            if (diff > max) {
                max = diff;
            }
            if (diff < this.thresh) {
                this.earlyStop = false;
            }
            if (!ordered.containsKey(sample)) {
                ordered.put(diff, new ArrayList<ArrayList<Integer>>());
            }
            ordered.get(diff).add(sample);
        }
        if (avaCounts.get(this.referenceTable.get(this.curTable).get(k)).containsKey(tweetDegree)) {
            ordered.put(0, new ArrayList<ArrayList<Integer>>());
            ordered.get(0).add(tweetDegree);
        }
        return ordered;
    }

    private ArrayList<ArrayList<Integer>> fetchSingle(int starter, int value, ArrayList<ArrayList<ArrayList<Integer>>> permutation) {
        ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
        int temp = (int) starter;
        int tvalue = value;
        for (int i = 0; i < permutation.size(); i++) {
            int ind = temp / (tvalue / permutation.get(i).size());
            preVdegrees.add(permutation.get(i).get(ind));
            tvalue = tvalue / permutation.get(i).size();
            temp = temp % tvalue;
        }
        return preVdegrees;
    }

    private void twoElement(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, ArrayList<ArrayList<Integer>> preVdegrees, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int k, int arrSize) {
        for (int i = 0; i < k; i++) {
            pair1.add(preVdegrees.get(i));
            pair2.add(entry.getKey().get(i));
        }
        pair2.add(preVdegrees.get(k));
        pair1.add(entry.getKey().get(k));

        for (int i = k + 1; i < arrSize; i++) {
            pair1.add(preVdegrees.get(i));
            pair2.add(entry.getKey().get(i));
        }
    }

    private int ivConstraint(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con, int bound1, int bound2, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int val) {

        int genVal = con.get(entry.getKey());

        if (!con.containsKey(pair1)) {
            con.put(pair1, 0);
        }
        if (!con.containsKey(pair2)) {
            con.put(pair2, 0);
        }
        int v1 = bound1 - con.get(pair1);
        int v2 = bound2 - con.get(pair2);
        int vsub = Math.min(v1, v2);
        vsub = Math.min(vsub, genVal);
        vsub = Math.min(vsub, val);
        return vsub;
    }

    private int updateCon(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, int vsub, int val, int bound1, int bound2, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con) {
        con.put(pair1, vsub + con.get(pair1));
        con.put(pair2, vsub + con.get(pair2));
        val = val - vsub;

        if (con.get(pair1) == allBounds.get(pair1)) {
            allBounds.remove(pair1);
            this.boundTrash.add(pair1);
        }
        if (con.get(pair2) == allBounds.get(pair2)) {
            allBounds.remove(pair2);
            this.boundTrash.add(pair2);
        }
        if (bound1 < con.get(pair1)) {
            System.err.println("pair1: " + pair1 + "    " + bound1 + "  " + con.get(pair1));
        }
        if (bound2 < con.get(pair2)) {
            System.err.println("pair2: " + pair2 + "    " + bound2 + "  " + con.get(pair2));
        }
        return val;
    }
    int originalValue = 0;
   int total_test = 0;
        int totalV_test = 0;
      
    private void mappingAllValueAesc(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        iteration++;
        HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders = new HashMap<>();
        if (sorted.size() <= 0) {
            return;
        }totalV_test = 0;
        total_test = 0;
        int length = sorted.get(0).getKey().size();
       for (int i = 0; !avaCounts.isEmpty() && i < sorted.size() && checkAvaCoutsEmpty() && sorted.size() > 0; i++) {
            Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
          
            
            int value = calValue(entry.getValue());
            totalV_test += entry.getValue();
           
            this.originalValue = value;
            if (value == 0) {
                continue;
            }
            if (iteration != 1) {
                updateValue(transOrders, entry.getKey(), result, value);
            }
            if (iteration == 1) {
                updateValueZero(transOrders, entry.getKey(), result, value);
            }
            //   }
        }

        System.err.println("calculated:" + this.totalSum + "   total:" + totalV_test);
    }
    int thresh = 5;

    private void updateValue(HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
        ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
        int totalNum = 1;
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            String sourceTable = this.referenceTable.get(curTable).get(i).sourceTable;

            if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(key.get(i)) == null) {
                return;
            }
            ArrayList<ArrayList<Integer>> arr = new ArrayList<ArrayList<Integer>>();
            for (ArrayList<Integer> temp : this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(key.get(i))) {
                arr.add(temp);
            }
            degreeSets.add(arr);
            totalNum *= degreeSets.get(i).size();
        }
        //System.out.println(totalNum + "  totalNum" );
        ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
        int startCount = -1;
        boolean flag;

        budget = value;

        while (startCount <= totalNum - 1 && budget > 0 && totalNum > 0) {
            startCount++;
            calDegrees = new ArrayList<>();
            int curNum = startCount;
            flag = true;

            calCandidateDegrees(calDegrees, curNum, totalNum, degreeSets);

            if (calDegrees.size() != this.referenceTable.get(curTable).size()) {
                System.err.println("CalDegree size not match");
                break;
            }

            flag = checkDegreeValid(calDegrees);
            if (!flag || calDegrees.size() == 0) {
                continue;
            }
            processCalDegrees(calDegrees, value, key, result);

        }

        return;
    }

    private void updateValueold(HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
        ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
        ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
        ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> calNormMaps = normMapCalculation(key);
        if (this.earlyStop) {
            return;
        }
        budget = value;
        for (int k = 0; k < thresh; k++) {
            if (k >= transOrders.size()) {
                break;
            }
            ArrayList<ArrayList<Integer>> pairs = transOrders.get(k);
            for (int j = 0; j < pairs.size(); j++) {
                if (budget == 0) {
                    return;
                }
                ArrayList<Integer> pair = pairs.get(j);

                boolean flag = checkFlag(pair, calNormMaps);
                if (!flag) {
                    continue;
                }

                degreeSets = new ArrayList<>();
                int totalNum = calDegreeSets(pair, degreeSets, calNormMaps);
                if (totalNum == 0) {
                    continue;
                }
                int startCount = -1;

                while (startCount < totalNum - 1 & budget > 0) {
                    startCount++;
                    calDegrees = new ArrayList<>();
                    int curNum = startCount;
                    flag = true;

                    calCandidateDegrees(calDegrees, curNum, totalNum, degreeSets);

                    if (calDegrees.size() != this.referenceTable.get(curTable).size()) {
                        System.err.println("CalDegree size not match");
                        break;
                    }

                    flag = checkDegreeValid(calDegrees);
                    if (!flag || calDegrees.size() == 0) {
                        continue;
                    }
                    processCalDegrees(calDegrees, value, key, result);

                }
            }

        }
        return;
    }

    private boolean checkAvaCoutsEmpty() {
        return avaCounts.containsKey(this.referenceTable.get(this.curTable).get(0)) && !avaCounts.get(this.referenceTable.get(this.curTable).get(0)).isEmpty();
    }

    private int calValue(Integer value) {
        int result = (int) (value * stime * (1 + Math.max(0, se)));
        double difff = value * stime * (1 + Math.max(0, se)) - result;
        double kl = Math.random();
        if (kl < difff) {
            result++;
        }
        return result;
    }

    private void updateValueZero(HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
        if (!checkDegreeValid(key)) {
            System.out.println("Return Early");
            return;
        }
        processCalDegrees(key, value, key, result);
        return;
    }

    private void updateIndividualValue(ArrayList<ArrayList<Integer>> calDegrees, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value, ArrayList<ArrayList<Integer>> key) {

        int oldV = 0;
        if (result.containsKey(calDegrees)) {
            oldV = result.get(calDegrees);
        }

        if (this.uniqueNess.get(curTable)) {
            int bound1 = calUperBound(calDegrees);

            if (oldV + value >= bound1) {
                this.allBounds.remove(calDegrees);
                this.boundTrash.add(calDegrees);
                value = bound1 - oldV;

                if ((this.level == 1)) {
                    System.out.println("Upper Bound Error!");
                }
            }
        }

        if (eveNum && (chechEqual(calDegrees.get(0),calDegrees.get(1)))) {
            value = value / 2;
        }
        if (this.originalValue != value && (!calDegrees.get(0).equals(calDegrees.get(1)))) {
            System.out.println("Error Here: " + calDegrees + "  " + value + "   " + this.originalValue);

        }
        if (value <= 0) {
            value = 0;
            return;
        }

        if (value > 0) {
            this.posValue = true;
        }

        result.put(calDegrees, value + oldV);

        for (int k = 0; k < calDegrees.size(); k++) {
            int v = avaCounts.get(referenceTable.get(curTable).get(k)).get(calDegrees.get(k));
            avaCounts.get(referenceTable.get(curTable).get(k)).put(calDegrees.get(k), v - value);
        }
        if (this.eveNum && (chechEqual(calDegrees.get(0),calDegrees.get(1)))) {
            budget = budget - 2 * value;
        } else {
            budget = budget - value;
        }
        if (this.eveNum &&(chechEqual(calDegrees.get(0),calDegrees.get(1)))) {
            this.totalSum += 2 * value;
        } else {
            this.totalSum += value;
        }//  System.out.println("value:"+value);
 if( totalSum!=(totalV_test*2) && iteration == 1 && this.eveNum )
           {
               System.out.println(key + "   " + this.originalValue + " " + value + "  " + totalSum + "   " + totalV_test + "  " + calDegrees);
               System.exit(-1);
           }
        
        for (int k = 0; k < calDegrees.size(); k++) {
            if (checkAvaCounts(calDegrees.get(k), k)) {
                avaCounts.get(referenceTable.get(curTable).get(k)).remove(calDegrees.get(k));
                if (k >= key.size()) {
                    System.out.println(k + " " + curTable);
                    System.exit(-1);
                }
                String sourceTable = this.referenceTable.get(curTable).get(k).sourceTable;
                if (!this.mappedBestJointDegree.containsKey(this.mergedDegreeTitle.get(sourceTable))) {
                    System.out.println(this.mergedDegreeTitle.get(sourceTable));
                    System.out.println(this.mappedBestJointDegree.keySet());
                    System.exit(-1);
                }
                if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(key.get(k))) {
                    if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(key.get(k)).contains(calDegrees.get(k))) {
                        this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(key.get(k)).remove(calDegrees.get(k));
                    }
                }
                cleanDistanceMap(k, calDegrees);
            }
        }
    }

    private boolean checkSocial(ArrayList<ArrayList<Integer>> calDegrees) {
        return !eveNum
                || (eveNum && checkOrder(calDegrees));

    }
    
    

    private int calUperBound(ArrayList<ArrayList<Integer>> calDegrees) {
        int bound1 = 1;

        if (this.allBounds.containsKey(calDegrees)) {
            bound1 = this.allBounds.get(calDegrees);
        } else if (calDegrees.size() == 1) {
            bound1 = Integer.MAX_VALUE;
        } else {
            for (int t = 0; t < calDegrees.size(); t++) {
                String sourceTable = this.referenceTable.get(curTable).get(t).sourceTable;
                if (downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t)) == null) {
                    System.out.println(calDegrees);
                    System.out.println(downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)));
                }
                if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t))) {
                    bound1 = Integer.MAX_VALUE;
                } else {
                    bound1 *= downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t));
                }
            }
            allBounds.put(calDegrees, bound1);
        }

        return bound1;
    }

    private boolean checkAvaCounts(ArrayList<Integer> degree, int k) {
        return (avaCounts.get(referenceTable.get(curTable).get(k)).containsKey(degree) && avaCounts.get(referenceTable.get(curTable).get(k)).get(degree) == 0);
    }

    private int degreeSum(ArrayList<Integer> degree) {
        int insum = 0;
        for (int jj : degree) {
            insum += jj;
        }
        return insum;
    }

    private void cleanDistanceMap(int k, ArrayList<ArrayList<Integer>> calDegrees) {
        int insum = degreeSum(calDegrees.get(k));
        if (!distanceMap.containsKey(referenceTable.get(curTable).get(k))) {

            System.out.println(referenceTable.get(curTable) + "   " + k);
            System.out.println(distanceMap.keySet());
            System.exit(-1);
        }
        if (!distanceMap.get(referenceTable.get(curTable).get(k)).containsKey(insum)) {
            System.out.print("Error 1 442" + calDegrees);
            System.out.println(this.distanceMap.get(referenceTable.get(curTable).get(k)));
            System.exit(-1);
        }
        if (!this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).contains(calDegrees.get(k))) {
            System.out.println("Error 2" + calDegrees);
            System.out.println(this.distanceMap.get(referenceTable.get(curTable).get(k)));

            System.exit(-1);
        }
        if (this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).isEmpty()) {
            this.distanceMap.get(referenceTable.get(curTable).get(k)).remove(insum);
        }
    }

    boolean earlyStop = true;

    private ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> normMapCalculation(ArrayList<ArrayList<Integer>> key) {
        ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> calNormMaps = new ArrayList<>();
        for (int k = 0; k < key.size(); k++) {
            int sumde = 0;
            for (int v : key.get(k)) {
                sumde += v;
            }
            ArrayList<ArrayList<Integer>> tempDegrees = new ArrayList<ArrayList<Integer>>();
            earlyStop = true;
            for (int i = -1 * thresh; i <= thresh; i++) {
                //   if 
                if (distanceMap.get(this.referenceTable.get(curTable).get(k)).containsKey(i + sumde)) {
                    for (ArrayList<Integer> r : distanceMap.get(this.referenceTable.get(curTable).get(k)).get(i + sumde)) {
                        tempDegrees.add(r);
                        earlyStop = false;
                    }
                }
            }
            if (earlyStop) {
                return null;
            }
            earlyStop = true;
            calNormMaps.add(norm1Closest(key.get(k), tempDegrees, k));
            if (earlyStop) {
                return null;
            }

        }
        return calNormMaps;
    }

    private boolean checkFlag(ArrayList<Integer> pair, ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> calNormMaps) {
        for (int i = 0; i < pair.size(); i++) {
            if (!calNormMaps.get(i).containsKey(pair.get(i))) {
                return false;
            }
        }
        return true;
    }

    private int calDegreeSets(ArrayList<Integer> pair, ArrayList<ArrayList<ArrayList<Integer>>> degreeSets, ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> calNormMaps) {
        int totalNum = 1;
        for (int i = 0; i < pair.size(); i++) {
            ArrayList<ArrayList<Integer>> calset = calNormMaps.get(i).get(pair.get(i));
            if (calset == null) {
                return 0;

            }
            degreeSets.add(calset);
            totalNum *= calset.size();
        }
        return totalNum;
    }

    private void calCandidateDegrees(ArrayList<ArrayList<Integer>> calDegrees, int curNum, int totalNum, ArrayList<ArrayList<ArrayList<Integer>>> degreeSets) {
        for (int h = 0; h < degreeSets.size(); h++) {
            int index = curNum % degreeSets.get(h).size();
            if (index == degreeSets.get(h).size()) {
                System.out.println(degreeSets + "   " + curNum + "    " + totalNum);
            }
            calDegrees.add(degreeSets.get(h).get(index));
            //curNum = curNum % (totalNum / degreeSets.get(h).size());
        }

    }

    private boolean checkDegreeValid(ArrayList<ArrayList<Integer>> calDegrees) {
        for (int i = 0; i < calDegrees.size(); i++) {
            if (!avaCounts.get(referenceTable.get(curTable).get(i)).containsKey(calDegrees.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void processCalDegrees(ArrayList<ArrayList<Integer>> calDegrees, int value, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int oldvalue = value;
        for (int i = 0; i < calDegrees.size(); i++) {
            errorChecking(i, calDegrees);

            value = Math.min(value, avaCounts.get(this.referenceTable.get(this.curTable).get(i)).get(calDegrees.get(i)));
        }
        if (value != oldvalue && this.eveNum) {
            System.out.println("Error Min: " + calDegrees + "  " + oldvalue + "   " + value);
        }
        if (value == 0) {
            return;
        }

        if (!this.boundTrash.contains(calDegrees) && checkSocial(calDegrees)) {
            updateIndividualValue(calDegrees, result, value, key);
            value = this.budget;
        }

    }
    boolean found = false;

    private void randomRound(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumthird = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        int value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();
        value = createPermutation(permutation);

        int starter = 0;
        boolean remov = false;

        while (starter < value && this.checkAvaCoutsEmpty() && permutation.get(0).size() > 0) {

            ArrayList<ArrayList<Integer>> preVdegrees = fetchSingle(starter, value, permutation);
            found = false;
            int val = 0;  //how many edges can be added
            val = firstCheck(preVdegrees);

            if (val > 0) {
                if (!this.boundTrash.contains(preVdegrees)) {
                    int bound = val;
                    if (this.uniqueNess.get(curTable)) {
                        bound = findUniqBound(preVdegrees);
                    }
                    val = processNewEdges(val, preVdegrees, con, bound);

                    sumthird += val;

                    if (found) {
                        for (int t = 0; t < preVdegrees.size(); t++) {
                            int oldVal = avaCounts.get(referenceTable.get(curTable).get(t)).get(preVdegrees.get(t));
                            avaCounts.get(referenceTable.get(curTable).get(t)).put(preVdegrees.get(t), oldVal - val);
                        }

                        for (int t = 0; t < preVdegrees.size(); t++) {
                            if (checkAvaCounts(preVdegrees.get(t), t)) {
                                avaCounts.get(referenceTable.get(curTable).get(t)).remove(preVdegrees.get(t));
                                remov = true;
                            }
                        }

                        if (remov) {
                            starter = -1;
                            value = createPermutation(permutation);
                        }
                    }

                }
            }
            starter++;
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        //    System.out.println(avaAttributes + "sumthird:" + sumthird);
        this.totalSum += sumthird;
    }

    private int createPermutation(ArrayList<ArrayList<ArrayList<Integer>>> permutation) {
        int value = 1;
        permutation.clear();
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            arr.addAll(avaCounts.get(referenceTable.get(curTable).get(i)).keySet());
            permutation.add(arr);
            value *= permutation.get(i).size();
        }
        return value;
    }

    private int firstCheck(ArrayList<ArrayList<Integer>> preVdegrees) {
        //Check the values are valid or not
        int result = Integer.MAX_VALUE;
        for (int t = 0; t < preVdegrees.size(); t++) {
            int vv = avaCounts.get(referenceTable.get(curTable).get(t)).get(preVdegrees.get(t));
            result = Math.min(result, vv);
            // avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);
            if (vv <= 0) {
                return 0;
            } else if ((eveNum && this.sumVector(preVdegrees.get(0)) > this.sumVector(preVdegrees.get(1)))) {
                return 0;
            } else if (eveNum && this.sumVector(preVdegrees.get(0)) == this.sumVector(preVdegrees.get(1))) {
                if (preVdegrees.get(0).equals(preVdegrees.get(1)) && preVdegrees.get(0).get(2) == 1) {

                } else {
                    return 0;
                }
            }
        }
        return result;
    }

    private int findUniqBound(ArrayList<ArrayList<Integer>> preVdegrees) {
        int bound1 = 1;

        if (this.allBounds.containsKey(preVdegrees)) {
            bound1 = this.allBounds.get(preVdegrees);
        } else if (preVdegrees.size() == 1) {
            bound1 = Integer.MAX_VALUE;
        } else {
            for (int t = 0; t < preVdegrees.size(); t++) {
                String sourceTable = this.referenceTable.get(curTable).get(t).sourceTable;
                if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(preVdegrees.get(t))) {
                    bound1 = Integer.MAX_VALUE;
                } else {
                    bound1 *= downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(preVdegrees.get(t));
                }
            }
            allBounds.put(preVdegrees, bound1);
        }
        return bound1;
    }

    private int processNewEdges(int val, ArrayList<ArrayList<Integer>> preVdegrees, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con, int bound) {
        if (con.containsKey(preVdegrees) && con.get(preVdegrees) < bound) {
            val = Math.min(val, bound - con.get(preVdegrees));
            con.put(preVdegrees, con.get(preVdegrees) + val);
            found = true;
        } else if (!con.containsKey(preVdegrees)) {
            val = Math.min(val, bound);
            con.put(preVdegrees, val);
            found = true;
        }

        if (con.get(preVdegrees) == this.allBounds.get(preVdegrees)) {
            this.allBounds.remove(preVdegrees);
            boundTrash.add(preVdegrees);
        }
        return val;
    }

    private void randSwap(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumn = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }

        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        int value = createPermutation(permutation);

        int starter = 0;
        boolean found0 = false;
        ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
        ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();
        int arrSize = referenceTable.get(curTable).size();
        while (starter < value && this.checkAvaCoutsEmpty()) {
            found0 = false;
            ArrayList<ArrayList<Integer>> preVdegrees = fetchSingle(starter, value, permutation);

            int val = firstCheck(preVdegrees);
            if (val == 0) {
                starter++;
                continue;
            }
            for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
                if (entry.getValue() == 0 || val == 0) {
                    continue;
                }

                for (int k = 0; k < arrSize; k++) {
                    pair1.clear();
                    pair2.clear();

                    twoElement(pair1, pair2, preVdegrees, entry, k, arrSize);

                    int bound1 = 1, bound2 = 1;
                    if (!this.boundTrash.contains(pair1) && !this.boundTrash.contains(pair2)) {
                        bound1 = findUniqBound(pair1);
                        bound2 = findUniqBound(pair2);

                        boolean fla = false;

                        if (eveNum && this.sumVector(pair1.get(0)) <= this.sumVector(pair1.get(1)) && this.sumVector(pair2.get(0)) <= this.sumVector(pair2.get(1))) {
                            fla = true;
                        }

                        if ((!con.containsKey(pair1) || con.get(pair1) < bound1) && (!con.containsKey(pair2) || con.get(pair2) < bound2) && (!eveNum || (fla && (!entry.getKey().equals(pair1) && !entry.getKey().equals(pair2))))) {

                            int vsub = ivConstraint(pair1, pair2, con, bound1, bound2, entry, val);
                            if (vsub == 0) {
                                continue;
                            }
                            sumn += vsub;
                            val = updateCon(pair1, pair2, vsub, val, bound1, bound2, con);

                            found0 = false;
                            updateDegrees(preVdegrees, vsub);

                            if (found0) {
                                starter = -1;
                                value = createPermutation(permutation);
                            }

                            break;
                        }
                    }
                }
                if (found0) {
                    break;
                }
            }

            starter++;
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        this.totalSum += sumn;
    }

    private boolean updateDegrees(ArrayList<ArrayList<Integer>> preVdegrees, int vsub) {
        boolean found0 = false;
        for (int t = 0; t < preVdegrees.size(); t++) {
            int vv = avaCounts.get(referenceTable.get(curTable).get(t)).get(preVdegrees.get(t));
            if (vv - vsub == 0) {
                found0 = true;
                avaCounts.get(referenceTable.get(curTable).get(t)).remove(preVdegrees.get(t));
            } else {
                avaCounts.get(referenceTable.get(curTable).get(t)).put(preVdegrees.get(t), vv - vsub);
            }
        }
        return found0;
    }

    private void errorChecking(int i, ArrayList<ArrayList<Integer>> calDegrees) {
        if (avaCounts.get(this.referenceTable.get(this.curTable).get(i)) == null) {
            String total = "";
            for (ComKey ck : avaCounts.keySet()) {
                total += "\t" + ck.toString();
            }
            System.err.println("TYPE1 ERROR line504 ParaCorrMap  " + this.referenceTable.get(this.curTable) + "     "
                    + this.referenceTable.get(this.curTable).get(i).toString() + "                " + total);
        } else if (avaCounts.get(this.referenceTable.get(this.curTable).get(i)).get(calDegrees.get(i)) == null) {
            String total = "";
            for (ComKey ck : avaCounts.keySet()) {
                total += "\t" + ck.toString();
            }
            System.err.println("TYPE1 ERROR 2ine504 ParaCorrMap  " + this.referenceTable.get(this.curTable) + "     "
                    + this.referenceTable.get(this.curTable).get(i).toString() + "                " + total + "           "
                    + "    i:" + i + "      " + calDegrees);

        }
    }

    private boolean checkOrder(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i=0;i<first.size();i++){
            if (first.get(i)>second.get(i)){
                return false;
            }
        }
        return true;
    }

    private boolean chechEqual(ArrayList<Integer> get, ArrayList<Integer> get0) {
        for (int i=0 ;i < get.size() ; i++){
            if (get.get(i)!=get0.get(i)){
                return false;
            }
        }
        return true;
    
}
}
