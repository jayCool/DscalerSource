package dsapara;

import dbstrcture.CoDa;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
public class ParaCorrMap implements Runnable {

    boolean eveNum = true;
    HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts;
    HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution;
    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    String ekey;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result;
    //List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted;
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
    CoDa originalCoDa;

    ParaCorrMap(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap,
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
    ArrayList<List<Entry<ArrayList<Integer>, Integer>>> sortedMerged = new ArrayList<>();
    ArrayList<ArrayList<Boolean>> fks = new ArrayList<>();

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> produceCorrMap() {
        iteration = 0;
        this.boundTrash.clear();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> result = new HashMap<>();
        allBounds = new HashMap<>();
        this.totalSum = 0;
        //System.out.println(avaCounts.keySet());
        for (ComKey ck : this.referenceTable.get(curTable)) {
            int sum = 0;
            for (int v : this.avaCounts.get(ck).values()) {
                sum += v;
            }
            System.out.println("curTable: " + this.curTable + "   ComKey: " + ck + "  total: " + sum);
        }
        Sort sort = new Sort();

        ArrayList<ArrayList<ComKey>> comkeys = new ArrayList<>();
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            ComKey refKey = this.referenceTable.get(this.curTable).get(i);
            String srcTable = refKey.sourceTable;
            comkeys.add(this.mergedDegreeTitle.get(srcTable));

            sortedMerged.add(sort.sortOnValueIntegerDesc(originalCoDa.mergedDistribution.get(comkeys.get(i))));
        }

        HashMap<ArrayList<ArrayList<Integer>>, Long> appearance = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> arr1 : this.originalCoDa.rvCorrelationDis.get(curTable).keySet()) {
            long sum1 = 1;
            for (int i = 0; i < arr1.size(); i++) {
                sum1 *= originalCoDa.mergedDistribution.get(comkeys.get(i)).get(arr1.get(i));
            }
            appearance.put(arr1, sum1);
        }

        List<Entry<ArrayList<ArrayList<Integer>>, Long>> sorted = sort.sortOnKeyAppearance(
               appearance, comkeys, this.referenceTable, this.originalCoDa.mergedDistribution);
      //  List<Entry<ArrayList<ArrayList<Integer>>, Long>> sorted = new ArrayList<>();
       // for (Entry<ArrayList<ArrayList<Integer>>, Long> entry: appearance.entrySet()){
        //    sorted.add(entry.);
    //        Entry<ArrayList<ArrayList<Integer>>, Long> temp = new
      //  }
        
        //(List<Entry<ArrayList<ArrayList<Integer>>, Long>>) appearance.entrySet();
        // List<Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<>();
        mappingAllValueAesc(result, sorted);

        // System.out.println(this.distanceMap.keySet());
        System.out.println("Iteration: " + this.iteration + "  " + this.curTable + " total:" + totalSum);
        while (iteration < 4 && checkAvaCoutsEmpty()) {
            level = (int) Math.pow(4, iteration);
            mappingAllValueAesc(result, sorted);
            System.out.println("Iteration: " + this.iteration + "  " + this.curTable + " total:" + totalSum);

        }

        int sum = 0;
        for (int i : result.values()) {
            sum += i;
        }
        System.out.println("First Finish: " + this.curTable + "  total: " + sum);
        iteration = 5;
        if (sorted.get(0).getKey().size() == 2) {
            if (checkAvaCoutsEmpty()) {
                randomRoundEffi(result);
            }

        }
        if (sorted.get(0).getKey().size() == 1) {
            if (checkAvaCoutsEmpty()) {
                randomRoundEffiOneKey(result);
            }
        }

        sum = 0;
        for (int i : result.values()) {
            sum += i;
        }
        System.out.println("Random Finish: " + this.curTable + "  total: " + sum);

        System.out.println("RandomMatch: " + this.curTable + " total:" + totalSum);

        if (sorted.get(0).getKey().size() == 2) {
            if (checkAvaCoutsEmpty()) {
                System.out.println("Start Radom Swap");
                randomSwapEffi(result);
            }
        }
        /* 
         if (checkAvaCoutsEmpty()) {
         System.out.println("Start Radom Swap");
         randSwap(result);
         }
         */
        if (checkAvaCoutsEmpty()) {
            System.out.println(this.curTable + ": is not empty!");
            //     for (int i = 0; i < this.referenceTable.get(this.curTable).size(); i++) {
            //       System.out.println(this.curTable + "   " + avaCounts.get(this.referenceTable.get(this.curTable).get(i)));
            //  }
        }

        sum = 0;
        for (int i : result.values()) {
            sum += i;
        }
        System.out.println("RandomSawp: " + this.curTable + " total:" + totalSum);
//        tableCorrelationDistribution
        System.out.println("Finish: " + this.curTable + "  total: " + sum);
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

    private ArrayList<ArrayList<Integer>> fetchSingle(long starter, long value, ArrayList<ArrayList<ArrayList<Integer>>> permutation) {
        ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();

        while (starterRandom < value) {
            long tempv = value;
            preVdegrees = new ArrayList<>();
            long temp = this.starterRandom;
            for (int i = 0; i < permutation.size(); i++) {
                int indexSize = calIndexSize(permutation, i);
                int ind = (int) (temp / indexSize);

                if (avaCounts.get(this.referenceTable.get(this.curTable).get(i)).containsKey(permutation.get(i).get(ind))) {
                    preVdegrees.add(permutation.get(i).get(ind));
                    temp = temp % indexSize;
                } else {
                    if (i == 0 && permutation.size() == 2) {
                        this.starterRandom = ((long) ind + 1) * (long) indexSize;
                    } else {

                        this.starterRandom++;
                    }
                    //  this.mappedBestJointDegree
                    break;
                }
            }
            if (preVdegrees.size() == permutation.size()) {

                return preVdegrees;
            }
        }
        return preVdegrees;
    }

    private boolean twoElement(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, ArrayList<ArrayList<Integer>> preVdegrees, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int k, int arrSize) {
        if (arrSize != entry.getKey().size()) {
            return false;
            //      System.out.println(this.curTable + "   " + preVdegrees + "    entry:" + entry);
            //     System.exit(-1);
        }
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
        return true;
    }

    private int ivConstraint(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con, int bound1, int bound2, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int val) {

        int genVal = con.get(entry.getKey());
        int pairV1 = 0, pairV2 = 0;
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

    int calValue = 0;

    private void mappingAllValueAesc(HashMap<ArrayList<ArrayList<Integer>>, Integer> result, List<Entry<ArrayList<ArrayList<Integer>>, Long>> sorted) {
        iteration++;
        HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders = new HashMap<>();
        totalV_test = 0;
        total_test = 0;

        int strictless = 0;
        int strictgreat = 0;
        int strictequal = 0;
        /*  for (int i=0; i <sorted.size(); i++){
         Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
         //}
         //for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : this.originalCoDa.rvCorrelationDis.get(this.curTable).entrySet()) {
         if (this.eveNum && this.checkStrictEqual(entry.getKey())) {
         strictequal += entry.getValue();
         }

         if (this.eveNum && this.checkStrictLess(entry.getKey())) {
         strictless += entry.getValue();
         }

         if (this.eveNum && this.checkStrictGreat(entry.getKey())) {
         strictgreat += entry.getValue();
         }

         }

         if (this.eveNum) {
         System.err.println("Strictly Less: " + strictless);
         System.err.println("Strictly Great: " + strictgreat);
         System.err.println("Strictly Equal: " + strictequal);

         }*/
        strictless = 0;
        strictgreat = 0;
        strictequal = 0;

        for (int i = 0; i < sorted.size(); i++) {

            //     for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : this.originalCoDa.rvCorrelationDis.get(this.curTable).entrySet()) {
            if (i == 0) {
            //    System.out.println(sorted.get(i));
            }
            int value = this.originalCoDa.rvCorrelationDis.get(this.curTable).get(sorted.get(i).getKey());
            if (!checkAvaCoutsEmpty()) {
                break;
            }
            /*if (this.eveNum && this.iteration == 1 && this.stime > 1) {
             System.out.println(this.curTable + "  :  " + entry);
             }*/
            if (value == 0) {
                continue;
            }
            value = calValue(value);
            calValue = value;
            if (this.eveNum && this.checkSocial(sorted.get(i).getKey())) {
                totalV_test += value;
            }

            if (this.eveNum && this.checkStrictEqual(sorted.get(i).getKey())) {
                strictequal += value;
            }

            if (this.eveNum && this.checkStrictLess(sorted.get(i).getKey())) {
                strictless += value;
            }

            if (this.eveNum && this.checkStrictGreat(sorted.get(i).getKey())) {
                strictgreat += value;
            }
            this.originalValue = value;
            if (value == 0) {
                if (this.eveNum && this.iteration == 1 && this.stime > 1) {
                    System.out.println("Value 0 Error ParaCorr265 " + sorted.get(i) + "  " + this.stime + "   ");
                }
                continue;
            }
            /*
             if (this.eveNum && this.iteration == 1 && this.stime > 1) {
             System.out.println(this.curTable + "  :  " + entry + "   valValue: " + value);
             }*/

            if (iteration != 1) {
                if (!eveNum || (this.eveNum && this.checkSocial(sorted.get(i).getKey()))) {
                    updateValue(transOrders, sorted.get(i).getKey(), result, value);
                }
            }
            if (iteration == 1) {

                if (!eveNum || (this.eveNum && this.checkSocial(sorted.get(i).getKey()))) {

                    updateValueZero(transOrders, sorted.get(i).getKey(), result, value);
                }

            }
            //   }
        }

        System.err.println("calculated:" + this.totalSum + "   total:" + totalV_test);
        if (this.eveNum) {
            System.err.println("Strictly Less: " + strictless);
            System.err.println("Strictly Great: " + strictgreat);
            System.err.println("Strictly Equal: " + strictequal);

        }
    }
    int thresh = 5;
    boolean[] fk1;
    boolean[] fk2;

    private void updateValue(HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
        ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
        int totalNum = 1;
        boolean bothFound = false;
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            String sourceTable = this.referenceTable.get(curTable).get(i).sourceTable;
            if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(key.get(i))) {
                bothFound = true;
            }
        }
        if (!bothFound) {
            return;
        }

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            String sourceTable = this.referenceTable.get(curTable).get(i).sourceTable;

            ArrayList<ArrayList<Integer>> arr = new ArrayList<ArrayList<Integer>>();
            if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(key.get(i))) {
                for (ArrayList<Integer> temp : this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(key.get(i))) {
                    arr.add(new ArrayList<Integer>(temp));
                }
            } else if (arr.size() == 0) {

                arr = new ArrayList<>(getCloseest(avaCounts.get(referenceTable.get(curTable).get(i)).keySet(), key.get(i), referenceTable.get(curTable).get(i)));
                //      this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).put(key.get(i), new ArrayList<>(arr));
            }

            if (arr.size() == 0) {
                return;
            }

            degreeSets.add(arr);
            totalNum *= arr.size();
        }
        //System.out.println(totalNum + "  totalNum" );
        ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
        int startCount = -1;
        boolean flag;

        budget = value;
        this.starterRandom = -1;
        //  System.out.println(this.curTable + "   sorting");
        while (starterRandom < totalNum - 1 && budget > 0 && totalNum > 0) {
            starterRandom++;
            calDegrees = new ArrayList<>();
            int curNum = startCount;
            flag = true;
            calDegrees = this.fetchSingle(starterRandom, totalNum, degreeSets, key);
            // calCandidateDegrees(calDegrees, curNum, totalNum, degreeSets);
            if (this.starterRandom >= totalNum) {
                break;
            }
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
        if (value == 0) {
            value++;

        }
        return result;
    }

    private void updateValueZero(HashMap<Integer, ArrayList<ArrayList<Integer>>> transOrders, ArrayList<ArrayList<Integer>> key, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {

        if (!checkDegreeValid(key)) {
            if (this.stime > 1 && this.eveNum && iteration == 1) {
                System.out.println("Return Early");
                System.out.println(key + "  " + this.curTable);
                System.exit(-1);
            }
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

        int bound1 = 0;
        if (this.uniqueNess.get(curTable)) {
            bound1 = calUperBound(calDegrees);

            if (oldV + value >= bound1) {
                if ((this.iteration == 1 && this.eveNum && this.stime > 1)) {
                    System.out.println("Upper Bound Error!:  bound1: " + bound1 + "   oldv:" + value);
                }
                this.allBounds.remove(calDegrees);
                this.boundTrash.add(calDegrees);
                value = bound1 - oldV;

            }
        }

        if (eveNum && checkEQ(calDegrees.get(0), calDegrees.get(1))) {
            //  System.out.println("Downsizes: " + value + "   " + calDegrees);
            value = value / 2;
        }

        if (this.stime > 1 && iteration == 1 && this.eveNum && this.originalValue != value && checkEQ(calDegrees.get(0), calDegrees.get(1))) {
            System.out.println("Error Here: " + calDegrees + "  " + value + "   " + this.originalValue);

        }
        if (value <= 0) {
            if (this.eveNum && iteration == 1 && stime > 1) {
                System.out.println("Value is 0, quit Early");
            }
            value = 0;
            return;
        }

        if (value > 0) {
            this.posValue = true;
        }

        result.put(calDegrees, value + oldV);
        if (this.eveNum && checkEQ(calDegrees.get(0), calDegrees.get(1))) {

            this.totalSum += 2 * value;
        } else {
            this.totalSum += value;
        }
        /* if (this.eveNum && this.stime > 1 && iteration == 1) {
         System.out.println(calDegrees + "   original: " + this.originalValue + "   calValue: " + this.calValue + "   finalValue: " + value
         + "  calSum: " + totalSum + "   totalV: " + totalV_test + "  ava1: "
         + avaCounts.get(referenceTable.get(curTable).get(0)).get(calDegrees.get(0)) + "    ava2: "
         + avaCounts.get(referenceTable.get(curTable).get(1)).get(calDegrees.get(1)) + "    bound: " + bound1);
         }
         */
        //  System.out.println("value:"+value);
        if (totalSum != (totalV_test * 2) && iteration == 1 && this.eveNum && this.stime > 1) {
            System.out.println(key + "   " + this.originalValue + " " + value + "  " + totalSum + "   " + totalV_test + "  "
                    + avaCounts.get(referenceTable.get(curTable).get(0)).get(calDegrees.get(0)) + "    "
                    + avaCounts.get(referenceTable.get(curTable).get(1)).get(calDegrees.get(1)) + "    " + bound1);
            System.exit(-1);
        }
        for (int k = 0; k < calDegrees.size(); k++) {
            int v = avaCounts.get(referenceTable.get(curTable).get(k)).get(calDegrees.get(k));
            avaCounts.get(referenceTable.get(curTable).get(k)).put(calDegrees.get(k), v - value);
        }
        if (this.eveNum && this.checkEQ(calDegrees.get(0), calDegrees.get(1))) {
            budget = budget - 2 * value;
        } else {
            budget = budget - value;
        }

        if (this.totalSum % 100 == 0) {
            //       System.out.println(this.curTable + "   succesful " + value);
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
                    bound1 = bound1 * downsizedMergedDistribution.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t));
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

        if (distanceMap.get(referenceTable.get(curTable).get(k)).containsKey(insum)) {

            this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).remove(calDegrees);
            if (this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).isEmpty()) {
                this.distanceMap.get(referenceTable.get(curTable).get(k)).remove(insum);
            }
        }

    }

    private void cleanDistanceMapD(int k, ArrayList<Integer> calDegrees) {
        int insum = degreeSum(calDegrees);
        ComKey ck = referenceTable.get(curTable).get(k);
        if (distanceMap.containsKey(ck) && distanceMap.get(ck).containsKey(insum)) {

            this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).remove(calDegrees);
            if (this.distanceMap.get(referenceTable.get(curTable).get(k)).get(insum).isEmpty()) {
                this.distanceMap.get(referenceTable.get(curTable).get(k)).remove(insum);
            }
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
        // printTest(calDegrees, value);
        if (value != oldvalue && this.eveNum && this.stime > 1 && iteration == 1) {
            System.out.println("Error Min: " + calDegrees + "  " + oldvalue + "   " + value);
        }
        if (value == 0) {
            if (this.eveNum && iteration == 1 && this.stime > 1) {
                System.out.println("Value 0 Early Quit in ParaCorrMap 645");
            }
            return;
        }

        if (!this.uniqueNess.get(curTable) || !this.boundTrash.contains(calDegrees) && checkSocial(calDegrees)) {
            updateIndividualValue(calDegrees, result, value, key);
            value = this.budget;
        }

    }
    boolean found = false;
    long starterRandom = 0;

    private void randomRound(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumthird = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        // int value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();
        long totalCount = createPermutation(permutation);

        boolean remov = false;
        System.out.println("Total Permutation: " + totalCount + "   " + this.curTable);
        starterRandom = 0;
        while (starterRandom < totalCount && this.checkAvaCoutsEmpty() && permutation.get(0).size() > 0) {

            ArrayList<ArrayList<Integer>> preVdegrees = fetchSingle(starterRandom, totalCount, permutation);
            found = false;
            if (starterRandom >= totalCount) {
                break;
            }
            int val = 0;  //how many edges can be added
            if (checkDegreeValid(preVdegrees)) {
                val = firstCheck(preVdegrees);
                //  this.checkDegreeValid(preVdegrees)

                if (val > 0) {
                    if (!this.boundTrash.contains(preVdegrees) || !this.uniqueNess.get(curTable)) {
                        int bound = val;
                        if (this.uniqueNess.get(curTable)) {
                            bound = findUniqBound(preVdegrees);
                        }
                        val = processNewEdges(val, preVdegrees, con, bound);

                        if (found) {
                            if (this.eveNum && this.checkEQ(preVdegrees.get(0), preVdegrees.get(1))) {
                                sumthird += 2 * val;
                            } else {
                                sumthird += val;
                            }
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

                            if (remov && false) {
                                //  starter = -1;
                                permutation = new ArrayList<>();
                                totalCount = createPermutation(permutation);
                            }
                        }

                    }
                }
            }
            starterRandom++;
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        //    System.out.println(avaAttributes + "sumthird:" + sumthird);
        this.totalSum += sumthird;
    }

    private void randomRoundEffi(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumthird = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        // int value = 1;
        ConcurrentHashMap<ArrayList<Integer>, Integer> id1 = new ConcurrentHashMap<>();
        ConcurrentHashMap<ArrayList<Integer>, Integer> id2 = new ConcurrentHashMap<>();

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            for (ArrayList<Integer> key : avaCounts.get(referenceTable.get(curTable).get(i)).keySet()) {
                if (i == 0) {
                    id1.put(key, 1);
                }
                if (i == 1) {
                    id2.put(key, 1);
                }
            }
        }
        String table1 = this.referenceTable.get(curTable).get(0).sourceTable;
        String table2 = this.referenceTable.get(curTable).get(1).sourceTable;

        boolean remov = false;
        long uniqBound = Integer.MAX_VALUE;
        starterRandom = 0;
        int usedValue = 0;
        for (ArrayList<Integer> first : id1.keySet()) {
            boolean skiped = false;
            for (ArrayList<Integer> second : id2.keySet()) {
                if (!avaCounts.get(referenceTable.get(curTable).get(0)).containsKey(first) || avaCounts.get(referenceTable.get(curTable).get(0)).get(first) == 0) {
                    skiped = true;
                    break;
                }
                if (!avaCounts.get(referenceTable.get(curTable).get(1)).containsKey(second) || avaCounts.get(referenceTable.get(curTable).get(1)).get(second) == 0) {
                    id2.remove(second);
                    continue;
                }
                int avaCount = Math.min(avaCounts.get(referenceTable.get(curTable).get(0)).get(first), avaCounts.get(referenceTable.get(curTable).get(1)).get(second));

                if (avaCount <= 0) {
                    System.out.println("Something wrong in random pair");
                    continue;

                }
                if (this.uniqueNess.get(curTable)) {
                    long v1 = downsizedMergedDistribution.get(this.mergedDegreeTitle.get(table1)).get(first);
                    long v2 = downsizedMergedDistribution.get(this.mergedDegreeTitle.get(table2)).get(second);
                    uniqBound = v1 * v2;
                }

                ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
                preVdegrees.add(first);
                preVdegrees.add(second);

                usedValue = processNewEdges(avaCount, preVdegrees, con, uniqBound);
                if (usedValue == 0) {
                    continue;
                }

                if (this.eveNum && this.checkEQ(preVdegrees.get(0), preVdegrees.get(1))) {
                    sumthird += 2 * usedValue;
                } else {
                    sumthird += usedValue;
                }

                for (int t = 0; t < preVdegrees.size(); t++) {
                    int oldVal = avaCounts.get(referenceTable.get(curTable).get(t)).get(preVdegrees.get(t));
                    avaCounts.get(referenceTable.get(curTable).get(t)).put(preVdegrees.get(t), oldVal - usedValue);
                }

                for (int t = 0; t < preVdegrees.size(); t++) {
                    if (checkAvaCounts(preVdegrees.get(t), t)) {
                        avaCounts.get(referenceTable.get(curTable).get(t)).remove(preVdegrees.get(t));
                        remov = true;
                    }
                }
            }

        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        //    System.out.println(avaAttributes + "sumthird:" + sumthird);
        this.totalSum += sumthird;
    }

    private void randomSwapEffi(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumthird = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        // int value = 1;
        ConcurrentHashMap<ArrayList<Integer>, Integer> id1 = new ConcurrentHashMap<>();
        ConcurrentHashMap<ArrayList<Integer>, Integer> id2 = new ConcurrentHashMap<>();

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            for (ArrayList<Integer> key : avaCounts.get(referenceTable.get(curTable).get(i)).keySet()) {
                if (i == 0) {
                    id1.put(key, 1);
                }
                if (i == 1) {
                    id2.put(key, 1);
                }
            }
        }
        String table1 = this.referenceTable.get(curTable).get(0).sourceTable;
        String table2 = this.referenceTable.get(curTable).get(1).sourceTable;

        boolean remov = false;
        long uniqBound = Integer.MAX_VALUE;
        starterRandom = 0;
        int usedValue = 0;
        for (ArrayList<Integer> first : id1.keySet()) {
            boolean skiped = false;
            for (ArrayList<Integer> second : id2.keySet()) {
                if (!avaCounts.get(referenceTable.get(curTable).get(0)).containsKey(first) || avaCounts.get(referenceTable.get(curTable).get(0)).get(first) == 0) {
                    skiped = true;
                    break;
                }
                if (!avaCounts.get(referenceTable.get(curTable).get(1)).containsKey(second) || avaCounts.get(referenceTable.get(curTable).get(1)).get(second) == 0) {
                    id2.remove(second);
                    continue;
                }
                int avaCount = Math.min(avaCounts.get(referenceTable.get(curTable).get(0)).get(first), avaCounts.get(referenceTable.get(curTable).get(1)).get(second));

                for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
                    if (entry.getValue() == 0 || entry.getKey().size() == 0) {
                        break;
                    }

                    ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
                    preVdegrees.add(first);
                    preVdegrees.add(second);

                    avaCount = loopPairs(preVdegrees, entry, 2, avaCount, con);

                    if (avaCount == 0) {
                        break;
                    }
                }

            }

        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        //    System.out.println(avaAttributes + "sumthird:" + sumthird);
        this.totalSum += sumthird;
    }

    private void randomRoundEffiOneKey(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumthird = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        // int value = 1;
        ConcurrentHashMap<ArrayList<Integer>, Integer> id1 = new ConcurrentHashMap<>();

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            for (ArrayList<Integer> key : avaCounts.get(referenceTable.get(curTable).get(i)).keySet()) {
                if (i == 0) {
                    id1.put(key, 1);
                }

            }
        }
        String table1 = this.referenceTable.get(curTable).get(0).sourceTable;

        boolean remov = false;
        long uniqBound = Integer.MAX_VALUE;
        starterRandom = 0;
        int usedValue = 0;
        for (ArrayList<Integer> first : id1.keySet()) {
            boolean skiped = false;
            if (!avaCounts.get(referenceTable.get(curTable).get(0)).containsKey(first) || avaCounts.get(referenceTable.get(curTable).get(0)).get(first) == 0) {
                skiped = true;
                break;
            }

            int avaCount = avaCounts.get(referenceTable.get(curTable).get(0)).get(first);

            if (avaCount <= 0) {
                System.out.println("Something wrong in random pair");
                continue;

            }

            ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
            preVdegrees.add(first);
            int old = 0;
            if (con.containsKey(preVdegrees)) {
                old = con.get(preVdegrees);
            }
            con.put(preVdegrees, old + avaCount);
            avaCounts.get(referenceTable.get(curTable).get(0)).remove(first);

        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        //    System.out.println(avaAttributes + "sumthird:" + sumthird);
        this.totalSum += sumthird;
    }

    private long createPermutation(ArrayList<ArrayList<ArrayList<Integer>>> permutation) {
        long value = 1;

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            for (Entry<ArrayList<Integer>, Integer> entry : this.sortedMerged.get(i)) {
                if (avaCounts.get(referenceTable.get(curTable).get(i)).containsKey(entry.getKey())) {
                    arr.add(entry.getKey());
                }
            }
            // permutation.add(new ArrayList<>(avaCounts.get(referenceTable.get(curTable).get(i)).keySet()));

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
            if (result <= 0) {
                return 0;
            } else if ((eveNum && !this.checkOrder(preVdegrees) && iteration == 1)) {
                return 0;
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

    private int processNewEdges(int val, ArrayList<ArrayList<Integer>> preVdegrees, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con,
            long bound) {
        if (con.containsKey(preVdegrees) && con.get(preVdegrees) < bound) {
            int old = con.get(preVdegrees);
            val = (int) Math.min((long) val, bound - old);
            if (this.eveNum && this.checkEQ(preVdegrees.get(0), preVdegrees.get(1))) {
                val = val / 2;
            }
            con.put(preVdegrees, old + val);
            found = true;
        } else if (!con.containsKey(preVdegrees)) {
            val = (int) Math.min((long) val, bound);
            if (this.eveNum && this.checkEQ(preVdegrees.get(0), preVdegrees.get(1))) {
                val = val / 2;
            }
            con.put(preVdegrees, val);
            found = true;
        }

        if (con.get(preVdegrees) == this.allBounds.get(preVdegrees)) {
            this.allBounds.remove(preVdegrees);
            boundTrash.add(preVdegrees);
        }
        return val;
    }
    boolean found0 = false;

    private void randSwap(HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        int sumn = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }

        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        long value = createPermutation(permutation);

        //   long starter = 0;
        this.starterRandom = 0;
        int arrSize = referenceTable.get(curTable).size();
        while (starterRandom < value && this.checkAvaCoutsEmpty()) {
            found0 = false;
            ArrayList<ArrayList<Integer>> preVdegrees = fetchSingle(starterRandom, value, permutation);
            if (starterRandom >= value) {
                break;
            }
            if (checkDegreeValid(preVdegrees)) {
                int val = firstCheck(preVdegrees);
                if (val == 0) {
                    starterRandom++;
                    continue;
                }
                for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
                    if (entry.getValue() == 0 || val == 0) {
                        break;
                    }
                    if (entry.getKey().size() == 0) {
                        continue;
                    }
                    val = loopPairs(preVdegrees, entry, arrSize, val, con);

                    if (found0 && val == 0) {
                        break;
                    }
                }

            }

            starterRandom++;
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
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return false;
            } else if (first.get(i) < second.get(i)) {
                return true;
            }
        }
        return true;
    }

    private boolean checkStrictGreat(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return true;
            } else if (first.get(i) < second.get(i)) {
                return false;
            }
        }
        return false;
    }

    private boolean checkStrictEqual(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        if (first.equals(second)) {
            return true;
        }
        return false;
    }

    private boolean checkStrictLess(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return false;
            } else if (first.get(i) < second.get(i)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkEQ(ArrayList<Integer> get, ArrayList<Integer> get0) {
        for (int i = 0; i < get.size(); i++) {
            if (!get.get(i).equals(get0.get(i))) {
                return false;
            }
        }
        return true;
    }

    private int calIndexSize(ArrayList<ArrayList<ArrayList<Integer>>> permutation, int i) {
        int size = 1;
        for (int k = i + 1; k < permutation.size(); k++) {
            size = size * permutation.get(k).size();
        }
        return size;
    }

    private void printTest(ArrayList<ArrayList<Integer>> calDegrees) {
        if (this.eveNum && iteration == 1 && this.stime > 1) {

        }
    }

    private void printTest(ArrayList<ArrayList<Integer>> calDegrees, int value) {
        System.out.println("Check: " + calDegrees + "  " + "   " + value);
    }

    private void loopPairs(ArrayList<ArrayList<Integer>> preVdegrees, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int arrSize, int val) {

    }

    private int loopPairs(ArrayList<ArrayList<Integer>> preVdegrees, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int arrSize, int avaCount, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con) {
        for (int k = 0; k < arrSize; k++) {
            ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
            ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();

            boolean rfa = twoElement(pair1, pair2, preVdegrees, entry, k, arrSize);
            if (!rfa) {
                break;
            }
            int bound1 = 1, bound2 = 1;

            if (!this.uniqueNess.get(curTable)) {
                bound1 = Integer.MAX_VALUE;
                bound2 = Integer.MAX_VALUE;
            } else {
                bound1 = findUniqBound(pair1);
                bound2 = findUniqBound(pair2);
            }
            if ((!con.containsKey(pair1) || con.get(pair1) < bound1)
                    && (!con.containsKey(pair2) || con.get(pair2) < bound2)) {

                int vsub = ivConstraint(pair1, pair2, con, bound1, bound2, entry, avaCount);
                if (vsub <= 0) {
                    continue;
                }
                this.totalSum += vsub;
                con.put(entry.getKey(), entry.getValue() - vsub);
                avaCount = updateCon(pair1, pair2, vsub, avaCount, bound1, bound2, con);

                found0 = false;
                found0 = updateDegrees(preVdegrees, vsub);

                break;

            }
        }

        return avaCount;
    }

    private ArrayList<ArrayList<Integer>> getCloseest(Set<ArrayList<Integer>> keySet, ArrayList<Integer> key, ComKey ck) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        int thresh = 10000;
        int sum = 0;
        for (int i : key) {
            sum += i;
        }
        for (int i = 1; i <= thresh; i++) {
            for (int k = -1; k <= 1; k = k + 2) {
                if (this.distanceMap.get(ck).containsKey(sum + k * i)) {
                    return distanceMap.get(ck).get(sum + k * i);
                    /*
                     for (ArrayList<Integer> sample : distanceMap.get(ck).get(sum + k * i)) {
                     int diff = computeDiff(key, sample);
                     if (diff > min) {
                     continue;
                     }
                     min = Math.min(min, diff);
                     if (diff <= min) {
                     result = new ArrayList<>();
                     result.add(sample);
                     }
                     }*/

                }
            }
            if (min <= i) {
                return result;
            }
        }

        return result;

    }

    private int computeDiff(ArrayList<Integer> key, ArrayList<Integer> sample) {
        int diff = 0;
        for (int i = 0; i < key.size(); i++) {
            diff += Math.abs(key.get(i) - sample.get(i));
        }
        return diff;
    }

    private ArrayList<ArrayList<Integer>> fetchSingle(long s, int value, ArrayList<ArrayList<ArrayList<Integer>>> permutation, ArrayList<ArrayList<Integer>> key) {
        ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
        while (starterRandom < value) {
            preVdegrees = new ArrayList<>();
            long temp = this.starterRandom;
            for (int i = 0; i < permutation.size(); i++) {
                int indexSize = calIndexSize(permutation, i);
                int ind = (int) (temp / indexSize);
                if (avaCounts.get(this.referenceTable.get(this.curTable).get(i)).containsKey(permutation.get(i).get(ind))) {

                    preVdegrees.add(permutation.get(i).get(ind));
                    temp = temp % indexSize;
                } else {
                    this.starterRandom++;
                    String srcTable = this.referenceTable.get(this.curTable).get(i).sourceTable;
                    if (mappedBestJointDegree.get(this.mergedDegreeTitle.get(srcTable)).containsKey(key.get(i))) {
                        mappedBestJointDegree.get(this.mergedDegreeTitle.get(srcTable)).get(key.get(i)).remove(permutation.get(i).get(ind));
                    }
                    this.cleanDistanceMapD(i, permutation.get(i).get(ind));
                    //  this.mappedBestJointDegree
                    break;
                }
            }
            if (preVdegrees.size() == permutation.size()) {

                return preVdegrees;
            }
        }
        return preVdegrees;
    }

}
