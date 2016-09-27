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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jiangwei
 *
 * This is most update correlation function for tweets and followee, and
 * follower
 */
public class CorrelationVertex extends PrintFunction {

    /**
     * @param args the command line arguments
     */
    public double stime = 0.2;

    public CorrelationVertex() {
    }

    private ArrayList<Integer> findClosest(ArrayList<Integer> pair, Set<ArrayList<Integer>> keySet) {
        if (keySet.contains(pair)) {
            return pair;
        }
        for (int i = 0; i < pair.size(); i++) {
            if (!pair.get(i).equals(0)) {
                for (int k = 1; k <= 10; k++) {
                    ArrayList<Integer> result = new ArrayList<Integer>();
                    for (int j = 0; j < pair.size(); j++) {
                        if (j == i) {
                            result.add(pair.get(j) - k);
                        } else {
                            result.add(pair.get(j));
                        }
                    }
                    if (keySet.contains(result)) {
                        return result;
                    }
                }
            }
        }

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
        int sumde = 0;
        for (int i : pair) {
            sumde += i;
        }

//System.out.println(keySet.size());
        for (ArrayList<Integer> temp : keySet) {
            int sumt = 0;
            for (int i : temp) {
                sumt += i;
            }
            if (!map.containsKey(Math.abs(sumt - sumde))) {
                map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(sumt - sumde)).add(temp);
        }

        //  System.out.println(map.size());
        if (map.size() == 0) {
            System.out.println(pair);
            System.out.println(keySet);
        }
        int closeSum = Collections.min(map.keySet());
        return map.get(closeSum).get((int) Math.random() * (map.get(closeSum).size() - 1));

    }

    private HashMap<Integer, ArrayList<ArrayList<Integer>>> closestMap(Set<ArrayList<Integer>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
        int sumde = 0;
        for (ArrayList<Integer> temp : keySet) {
            int sumt = 0;
            for (int i : temp) {
                sumt += i;
            }
            if (!map.containsKey(sumt)) {
                map.put(sumt, new ArrayList<ArrayList<Integer>>());
            }
            map.get(sumt).add(temp);
        }
        return map;
    }
    int indexcount = 0;

    private ArrayList<ArrayList<Integer>> normMapCandidate(HashMap<Integer, ArrayList<ArrayList<Integer>>> norm1Map, ArrayList<Integer> pair) {
        int sumde = 0;
        for (int i : pair) {
            sumde += i;
        }

        int max = Integer.MAX_VALUE;
        for (int i = 0; i < max; i++) {
            if (norm1Map.containsKey(i + sumde) && !norm1Map.get(i + sumde).isEmpty()) {
                indexcount = i + sumde;
                return norm1Map.get(indexcount);
            }
            if (norm1Map.containsKey(-i + sumde) && !norm1Map.get(-i + sumde).isEmpty()) {
                indexcount = -i + sumde;
                return norm1Map.get(indexcount);
            }

        }
        System.out.println("error");
        return null;
    }

    private ArrayList<ArrayList<Integer>> findClosestSet(ArrayList<Integer> pair, Set<ArrayList<Integer>> keySet) {

        TreeMap<Integer, ArrayList<ArrayList<Integer>>> map = new TreeMap<>();
        int sumde = 0;
        for (int i : pair) {
            sumde += i;
        }

//System.out.println(keySet.size());
        for (ArrayList<Integer> temp : keySet) {
            int sumt = 0;
            for (int i : temp) {
                sumt += i;
            }
            if (!map.containsKey(Math.abs(sumt - sumde))) {
                map.put(Math.abs(sumt - sumde), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(sumt - sumde)).add(temp);
        }

        //  System.out.println(map.size());
        if (map.size() == 0) {
            System.out.println(pair);
            System.out.println(keySet);
        }
        // int closeSum = Collections.min(map.keySet());
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        int size = 0;
        for (ArrayList<ArrayList<Integer>> arr : map.values()) {
            result.addAll(arr);
            size += arr.size();
            if (size >= 5) {
                break;
            }
        }
        return result;

    }

    double rationP = 0.005;
    String curTable = "";
    ArrayList<Integer> curIndexes = new ArrayList<>();

    HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> corrDist(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> tableCorrelationDistribution, HashMap<String, ArrayList<String>> mergedDegreeTitle, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio) {
        HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result = new HashMap<>();
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts = processAva(downsizedMergedDistribution);
        processDistanceMap(avaCounts);

        for (Entry<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry : tableCorrelationDistribution.entrySet()) {
            if (true || !entry.getKey().get(0).equals("socialgraph")) {
                ArrayList<ArrayList<String>> attributeOrders = new ArrayList<>();
                ArrayList<ArrayList<String>> avaAttributes = new ArrayList<>();
                curIndexes = new ArrayList<>();
                for (int i = 1; i < entry.getKey().size(); i++) {
                    ArrayList<String> arr1 = new ArrayList<>();
                    arr1.add(entry.getKey().get(i));
                    arr1.addAll(mergedDegreeTitle.get(entry.getKey().get(i)));
                    curIndexes.add(mergedDegreeTitle.get(entry.getKey().get(i)).indexOf(entry.getKey().get(0)));
                    attributeOrders.add(arr1);
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add(entry.getKey().get(i));
                    arr.add(entry.getKey().get(0));
                    avaAttributes.add(arr);
                }
                if (entry.getKey().get(0).equals("socialgraph")) {
                    eveNum = true;
                }
                //   System.out.println(avaCounts);
                curTable = entry.getKey().get(0);
                System.out.println(entry.getKey());
                HashMap<ArrayList<ArrayList<Integer>>, Integer> corred = produceCorrMap(avaCounts, entry.getValue(), attributeOrders, downsizedMergedDistribution, avaAttributes, downsizedMergedRatio, mergedDegreeTitle);
                result.put(entry.getKey(), corred);
                eveNum = false;
                System.out.println(avaAttributes.get(0));
                System.out.println("avaCounts" + avaCounts.get(avaAttributes.get(0)));
            }
        }

        return result;
    }
    public boolean eveNum = false;

    private HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> processAva(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution) {
        HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> result = new HashMap<>();
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : downsizedMergedDistribution.entrySet()) {
            for (int i = 1; i < entry.getKey().size(); i++) {
                ArrayList<String> keys = new ArrayList<>();
                keys.add(entry.getKey().get(0));
                keys.add(entry.getKey().get(i));
                HashMap<ArrayList<Integer>, Integer> ava = new HashMap<>();
                for (Entry<ArrayList<Integer>, Integer> entry2 : entry.getValue().entrySet()) {
                    if (entry2.getValue() * entry2.getKey().get(i - 1) > 0) {
                        ava.put(entry2.getKey(), entry2.getValue() * entry2.getKey().get(i - 1));
                    }
                }
                result.put(keys, ava);
            }
        }
        return result;
    }

    int iteration = 0;

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> produceCorrMapOld(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes) {
        iteration = 0;
        System.out.println("iteration: 0");
        this.boundTrash.clear();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> result = new HashMap<>();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();
        mapping(avaCounts, correlationFunction, attributeOrders, downsizedMergedDistribution, avaAttributes, result);
        while (iteration < 3 && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty()) {
            System.out.println("iteration: " + iteration);
            mapping(avaCounts, correlationFunction, attributeOrders, downsizedMergedDistribution, avaAttributes, result);
        }
        //    System.out.println("ava:"+avaCounts);
        System.out.println(boundTrash.size());
        if (!avaCounts.get(avaAttributes.get(0)).keySet().isEmpty()) {
            this.thirdRound(avaCounts, result, downsizedMergedDistribution, avaAttributes, attributeOrders);

            lastRound(avaCounts, result, downsizedMergedDistribution, avaAttributes, attributeOrders);
        }
// System.out.println("ava:"+avaCounts);
        if (eveNum) {
            //  System.out.println(avaCounts);
            //     System.out.println(result);
        }
        return result;
    }
    HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();

    HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> produceCorrMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        iteration = 0;
        System.out.println("iteration: 0");
        this.boundTrash.clear();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> result = new HashMap<>();
        allBounds = new HashMap<>();
        this.totalSum = 0;
        System.out.println(avaCounts);
        mappingAllValueAesc(avaCounts, correlationFunction, attributeOrders, downsizedMergedDistribution, avaAttributes, result, downsizedMergedRatio, mergedDegreeTitle);
        while (iteration < 3 && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty()) {
            System.out.println("iteration: " + iteration);
            mappingAllValueAesc(avaCounts, correlationFunction, attributeOrders, downsizedMergedDistribution, avaAttributes, result, downsizedMergedRatio, mergedDegreeTitle);
        }
        System.out.println(boundTrash.size());
        if (!avaCounts.get(avaAttributes.get(0)).keySet().isEmpty()) {
            thirdRound(avaCounts, result, downsizedMergedDistribution, avaAttributes, attributeOrders);

            this.lastRound(avaCounts, result, downsizedMergedDistribution, avaAttributes, attributeOrders);
        }
        return result;
    }

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mapping2(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        Sort so = new Sort();
        iteration++;
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = so.sortOnKeySum(correlationFunction);
        for (int i = 0; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            //   ArrayList<ArrayList<Integer>> reverseDegree = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (!eveNum || this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1))) {
                int totalNum = 1;
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<Integer> calDeg = findClosest(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    //         ArrayList<ArrayList<Integer>> calset = findClosestSet(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    //      degreeSets.add(calset);
                    //      totalNum *= calset.size();
                    calDegrees.add(calDeg);
                }

                ArrayList<Integer> indexes = new ArrayList<>();
                int startCount = 0;
                int curNum = startCount;
                while (startCount < totalNum) {
                    for (int h = 0; h < degreeSets.size(); h++) {
                        indexes.add(curNum / (totalNum / degreeSets.get(i).size()));
                        curNum = curNum % (totalNum / degreeSets.get(i).size());
                    }
                    startCount = totalNum;

                    if (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1)))) {
                        HashMap<ArrayList<String>, ArrayList<ArrayList<Integer>>> coun = new HashMap<>();
                        int value = (int) (entry.getValue() * stime);
                        double difff = entry.getValue() * stime - value;
                        double kl = Math.random();
                        if (kl < difff || value == 0) {
                            value++;
                        }

                        for (int k = 0; k < calDegrees.size(); k++) {
                            value = Math.min(value, avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)));
                            if (!coun.containsKey(avaAttributes.get(k))) {
                                coun.put(avaAttributes.get(k), new ArrayList<ArrayList<Integer>>());
                            }
                            coun.get(avaAttributes.get(k)).add(calDegrees.get(k));
                        }
//System.out.println(value);
                        if (value >= 1) {
                            for (Entry<ArrayList<String>, ArrayList<ArrayList<Integer>>> entry2 : coun.entrySet()) {
                                ArrayList<ArrayList<Integer>> ars = entry2.getValue();
                                HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
                                for (ArrayList<Integer> ar : ars) {
                                    if (!maps.containsKey(ar)) {
                                        maps.put(ar, 0);
                                    }
                                    maps.put(ar, maps.get(ar) + 1);
                                }
                                for (Entry<ArrayList<Integer>, Integer> entry3 : maps.entrySet()) {
                                    value = Math.min(value, avaCounts.get(entry2.getKey()).get(entry3.getKey()) / entry3.getValue());

                                }
                            }
                            if (value >= 1) {
                                int bound1 = 1, bound2 = 1;
                                for (int k = 0; k < calDegrees.size(); k++) {
                                    if (downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k)) < 0) {
                                        System.out.println(attributeOrders.get(k));
                                        //System.out.println(downsizedMergedDistribution.get(attributeOrders.get(k)));
                                        System.out.println(calDegrees.get(k));
                                        System.out.println(downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k)));

                                    }
                                    if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k))) {
                                        bound1 = Integer.MAX_VALUE;
                                    } else {
                                        bound1 *= downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k));
                                    }

                                }

                                if (!result.containsKey(calDegrees)) {
                                    result.put(calDegrees, 0);
                                }

                                if (result.get(calDegrees) + value > bound1) {
                                    value = bound1 - result.get(calDegrees);
                                }

                                if (bound1 <= 0) {
                                    System.out.println("bound1: " + bound1);
                                }
                                System.out.println("value:" + value + "entry:" + entry);
                                if (value < 0) {
                                    value = 0;
                                }

                                result.put(calDegrees, value + result.get(calDegrees));

                                if (result.get(calDegrees) > bound1) {
                                    System.out.println("bound: " + bound1 + "  " + result.get(calDegrees));
                                }
                                for (int k = 0; k < calDegrees.size(); k++) {
                                    avaCounts.get(avaAttributes.get(k)).put(calDegrees.get(k), avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) - value);
                                }

                                for (int k = 0; k < calDegrees.size(); k++) {
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) < 0) {
                                        //   
                                        System.out.println("Errorr: " + value + " " + calDegrees + " " + avaAttributes.get(k));
                                    }
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) == 0) {
                                        Math.min(value, avaCounts.get(avaAttributes.get(k)).remove(calDegrees.get(k)));
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }
    boolean posValue = false;
    HashSet<ArrayList<ArrayList<Integer>>> boundTrash = new HashSet<>();

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mapping(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        Sort so = new Sort();
        iteration++;
        //  boundTrash.clear();
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = so.sortOnKeySumDesc(correlationFunction);
        for (int i = 0; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            //   ArrayList<ArrayList<Integer>> reverseDegree = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (!eveNum || this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1))) {
                int totalNum = 1;
                for (int k = 0; k < entry.getKey().size(); k++) {
                    //      ArrayList<Integer> calDeg = findClosest(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    ArrayList<ArrayList<Integer>> calset = findClosestSet(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    degreeSets.add(calset);
                    totalNum *= calset.size();
                    //     calDegrees.add(calDeg);
                }
                if (totalNum == 0) {
                    break;
                }
                ArrayList<Integer> indexes = new ArrayList<>();
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                while (startCount < totalNum && !this.posValue) {
                    calDegrees.clear();
                    curNum = startCount;
                    for (int h = 0; h < degreeSets.size(); h++) {
                        calDegrees.add(degreeSets.get(h).get(curNum / (totalNum / degreeSets.get(h).size())));
                        //   indexes.add(curNum/(totalNum/degreeSets.get(i).size()));
                        curNum = curNum % (totalNum / degreeSets.get(h).size());
                    }
                    startCount++;
                    //     System.out.println(startCount);
                    if (!this.boundTrash.contains(calDegrees) && (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1))))) {
                        HashMap<ArrayList<String>, ArrayList<ArrayList<Integer>>> coun = new HashMap<>();
                        int value = (int) (entry.getValue() * stime);
                        double difff = entry.getValue() * stime - value;
                        double kl = Math.random();
                        if (kl < difff || value == 0) {
                            value++;
                        }

                        for (int k = 0; k < calDegrees.size(); k++) {
                            value = Math.min(value, avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)));
                            if (!coun.containsKey(avaAttributes.get(k))) {
                                coun.put(avaAttributes.get(k), new ArrayList<ArrayList<Integer>>());
                            }
                            coun.get(avaAttributes.get(k)).add(calDegrees.get(k));
                        }
//System.out.println(value);
                        if (value >= 1) {
                            for (Entry<ArrayList<String>, ArrayList<ArrayList<Integer>>> entry2 : coun.entrySet()) {
                                ArrayList<ArrayList<Integer>> ars = entry2.getValue();
                                HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
                                for (ArrayList<Integer> ar : ars) {
                                    if (!maps.containsKey(ar)) {
                                        maps.put(ar, 0);
                                    }
                                    maps.put(ar, maps.get(ar) + 1);
                                }
                                for (Entry<ArrayList<Integer>, Integer> entry3 : maps.entrySet()) {
                                    value = Math.min(value, avaCounts.get(entry2.getKey()).get(entry3.getKey()) / entry3.getValue());

                                }
                            }
                            if (value >= 1) {
                                int bound1 = 1, bound2 = 1;
                                for (int k = 0; k < calDegrees.size(); k++) {
                                    if (downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k)) < 0) {
                                        System.out.println(attributeOrders.get(k));
                                        //System.out.println(downsizedMergedDistribution.get(attributeOrders.get(k)));
                                        System.out.println(calDegrees.get(k));
                                        System.out.println(downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k)));

                                    }
                                    if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k))) {
                                        bound1 = Integer.MAX_VALUE;
                                    } else {
                                        bound1 *= downsizedMergedDistribution.get(attributeOrders.get(k)).get(calDegrees.get(k));
                                    }

                                }

                                if (!result.containsKey(calDegrees)) {
                                    result.put(calDegrees, 0);
                                }

                                if (result.get(calDegrees) + value > bound1) {
                                    value = bound1 - result.get(calDegrees);
                                }

                                if (bound1 <= 0) {
                                    System.out.println("bound1: " + bound1);
                                }
                                //  System.out.println("value:" + value + "entry:" + entry+" bound:"+bound1);
                                if (value <= 0) {
                                    value = 0;
                                    this.boundTrash.add(calDegrees);
                                }
                                if (value > 0) {
                                    this.posValue = true;
                                }
                                result.put(calDegrees, value + result.get(calDegrees));

                                if (result.get(calDegrees) > bound1) {
                                    System.out.println("bound: " + bound1 + "  " + result.get(calDegrees));
                                }
                                for (int k = 0; k < calDegrees.size(); k++) {
                                    avaCounts.get(avaAttributes.get(k)).put(calDegrees.get(k), avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) - value);
                                }

                                for (int k = 0; k < calDegrees.size(); k++) {
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) < 0) {
                                        //   
                                        System.out.println("Errorr: " + value + " " + calDegrees + " " + avaAttributes.get(k));
                                    }
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) == 0) {
                                        Math.min(value, avaCounts.get(avaAttributes.get(k)).remove(calDegrees.get(k)));
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    private void updateValue(ArrayList<ArrayList<Integer>> calDegrees, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
        if (Math.random() > 0.9999) {
            System.out.println(this.totalSum);
        }
        for (int k = 0; k < calDegrees.size(); k++) {

            value = Math.min(value, avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)));
        }
        if (value >= 1) {
            int bound1 = 1;
            if (this.allBounds.containsKey(calDegrees)) {
                bound1 = this.allBounds.get(calDegrees);;
            } else {
                for (int t = 0; t < calDegrees.size(); t++) {

                    if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(attributeOrders.get(t)).get(calDegrees.get(t))) {
                        bound1 = Integer.MAX_VALUE;
                    } else {
                        bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(calDegrees.get(t));
                    }
                }
                //only one FK
                if (calDegrees.size() == 1) {
                    bound1 = avaCounts.get(avaAttributes.get(0)).get(calDegrees.get(0));
                }

                allBounds.put(calDegrees, bound1);
            }
            if (!result.containsKey(calDegrees)) {
                result.put(calDegrees, 0);
            }
            if (result.get(calDegrees) + value >= bound1) {
                this.allBounds.remove(calDegrees);
                this.boundTrash.add(calDegrees);
                value = bound1 - result.get(calDegrees);
            }
            if (bound1 <= 0) {
                System.out.println("bound1: " + bound1);
            }
            if (value <= 0) {
                value = 0;
            }
            if (value > 0) {
                this.posValue = true;
            }
            if (eveNum && calDegrees.get(0).equals(calDegrees.get(1))) {
                value = value / 2;
            }
            result.put(calDegrees, value + result.get(calDegrees));
            if (result.get(calDegrees) > bound1) {
                System.out.println("bound: " + bound1 + "  " + result.get(calDegrees));
            }
            for (int k = 0; k < calDegrees.size(); k++) {
                avaCounts.get(avaAttributes.get(k)).put(calDegrees.get(k), avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) - value);
            }
            this.totalSum += value;

            for (int k = 0; k < calDegrees.size(); k++) {
                if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) == 0) {
                    avaCounts.get(avaAttributes.get(k)).remove(calDegrees.get(k));
                    int insum = 0;
                    for (int jj : calDegrees.get(k)) {
                        insum += jj;
                    }
                    this.distanceMap.get(avaAttributes.get(k)).get(insum).remove(calDegrees.get(k));
                    if (this.distanceMap.get(avaAttributes.get(k)).get(insum).isEmpty()) {
                        this.distanceMap.get(avaAttributes.get(k)).remove(insum);
                    }
                }

            }

        }
    }
    int totalSum = 0;
    Sort so = new Sort();

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mappingAllValueAesc(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        iteration++;
        System.out.println("startSorting");
        if (iteration <= 1) {
            System.out.println("corr:" + correlationFunction);
            sorted = so.sortOnKeyPosition(correlationFunction, this.curIndexes);
        }
        for (int i = 0; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (true) {
                int totalNum = 1;
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                int value = entry.getValue();
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<ArrayList<Integer>> calset = this.normMapCandidate(distanceMap.get(avaAttributes.get(k)), entry.getKey().get(k));
                    if (calset == null) {
                        System.out.println(this.totalSum);
                        for (int j = 0; j < entry.getKey().size(); j++) {
                            System.out.println(avaCounts.get(avaAttributes.get(j)));
                        }
                    }
                    if (calset.size() == 0) {
                        distanceMap.get(avaAttributes.get(k)).remove(this.indexcount);
                    }
                    degreeSets.add(calset);
                    totalNum *= calset.size();
                }
                if (totalNum == 0) {
                    break;
                }
                while (startCount < totalNum && !this.posValue) {
                    calDegrees.clear();
                    curNum = startCount;
                    for (int h = 0; h < degreeSets.size(); h++) {
                        calDegrees.add(degreeSets.get(h).get(curNum / (totalNum / degreeSets.get(h).size())));
                        curNum = curNum % (totalNum / degreeSets.get(h).size());
                    }
                    double rat = 0;
                    for (int k = 0; k < entry.getKey().size(); k++) {
                        rat = Math.max(rat, downsizedMergedRatio.get(attributeOrders.get(k)).get(calDegrees.get(k)));
                    }
                    value = (int) (entry.getValue() * rat);
                    if (value <= 0) {
                        value++;
                    }

                    startCount++;
                    if (!this.boundTrash.contains(calDegrees) && (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) <= this.sumVector(calDegrees.get(1)) && (calDegrees.get(0) != calDegrees.get(1) || calDegrees.get(0).get(2) == 1)))) {
                        updateValue(calDegrees, avaCounts, attributeOrders, downsizedMergedDistribution, avaAttributes, result, value);
                    }
                }
            }
            //   }
        }
        return result;
    }

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mappingAllUnSorted(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        iteration++;
        System.out.println("startSorting");
     //   Sort so = new Sort();

        //      List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<>();
        if (iteration <= 1) {
            //         sorted = so.sortOnValueAESC(correlationFunction);
            //
        }

        //  System.out.println("endSorting" + " " + sorted.size());
        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : correlationFunction.entrySet()) {
            //   ; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            if (avaCounts.get(avaAttributes.get(0)).keySet().isEmpty()) {
                break;
            }
            //   Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (!eveNum || this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1))) {
                int totalNum = 1;
                ArrayList<Integer> indexes = new ArrayList<>();
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                int value = (int) (entry.getValue() * stime);
                double difff = entry.getValue() * stime - value;
                double kl = Math.random();
                if (kl < difff || value == 0) {
                    value++;
                }
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<ArrayList<Integer>> calset = findClosestSet(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    degreeSets.add(calset);
                    totalNum *= calset.size();
                }
                if (totalNum == 0) {
                    break;
                }
                while (startCount < totalNum && !this.posValue) {
                    calDegrees.clear();
                    curNum = startCount;
                    for (int h = 0; h < degreeSets.size(); h++) {
                        calDegrees.add(degreeSets.get(h).get(curNum / (totalNum / degreeSets.get(h).size())));
                        curNum = curNum % (totalNum / degreeSets.get(h).size());
                    }
                    startCount++;
                    if (!this.boundTrash.contains(calDegrees) && (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1))))) {
                        updateValue(calDegrees, avaCounts, attributeOrders, downsizedMergedDistribution, avaAttributes, result, value);
                    }

                }
            }
            //   }
        }
        return result;
    }
    List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = new ArrayList<>();

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mappingAll(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        iteration++;
        System.out.println("startSorting");
        Sort so = new Sort();

        if (iteration <= 1) {
            sorted = so.sortOnKeySumDesc(correlationFunction);
            //
        }

        System.out.println("endSorting" + " " + sorted.size());
        for (int i = 0; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (!eveNum || this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1))) {
                int totalNum = 1;
                ArrayList<Integer> indexes = new ArrayList<>();
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                int value = (int) (entry.getValue() * stime);
                double difff = entry.getValue() * stime - value;
                double kl = Math.random();
                if (kl < difff || value == 0) {
                    value++;
                }

                /*    if (false && this.sumVector(entry.getKey().get(0)) < this.sumVector(entry.getKey().get(1)) && this.allBounds.containsKey(entry.getKey())) {
                 calDegrees = entry.getKey();
                 updateValue(calDegrees, avaCounts, attributeOrders, downsizedMergedDistribution, avaAttributes, result, value);
                 } else {
                 */
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<ArrayList<Integer>> calset = findClosestSet(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    degreeSets.add(calset);
                    totalNum *= calset.size();
                }
                if (totalNum == 0) {
                    break;
                }
                while (startCount < totalNum && !this.posValue) {
                    calDegrees.clear();
                    curNum = startCount;
                    for (int h = 0; h < degreeSets.size(); h++) {
                        calDegrees.add(degreeSets.get(h).get(curNum / (totalNum / degreeSets.get(h).size())));
                        curNum = curNum % (totalNum / degreeSets.get(h).size());
                    }
                    startCount++;
                    if (!this.boundTrash.contains(calDegrees) && (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1))))) {
                        updateValue(calDegrees, avaCounts, attributeOrders, downsizedMergedDistribution, avaAttributes, result, value);
                    }

                }
            }
            //   }
        }
        return result;
    }

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mappingAll2(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result) {
        Sort so = new Sort();
        iteration++;
        System.out.println("startSorting");

        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted = so.sortOnKeySumDesc(correlationFunction);
        System.out.println("endSorting" + " " + sorted.size());
        for (int i = 0; i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
            if (!eveNum || this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1))) {
                int totalNum = 1;
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<ArrayList<Integer>> calset = findClosestSet(entry.getKey().get(k), avaCounts.get(avaAttributes.get(k)).keySet());
                    degreeSets.add(calset);
                    totalNum *= calset.size();
                }
                if (totalNum == 0) {
                    break;
                }
                ArrayList<Integer> indexes = new ArrayList<>();
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                while (startCount < totalNum && !this.posValue) {
                    calDegrees.clear();
                    curNum = startCount;
                    for (int h = 0; h < degreeSets.size(); h++) {
                        calDegrees.add(degreeSets.get(h).get(curNum / (totalNum / degreeSets.get(h).size())));
                        curNum = curNum % (totalNum / degreeSets.get(h).size());
                    }
                    startCount++;
                    if (this.allBounds.containsKey(calDegrees) && (!eveNum || (eveNum && this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1))))) {
                        HashMap<ArrayList<String>, ArrayList<ArrayList<Integer>>> coun = new HashMap<>();
                        int value = (int) (entry.getValue() * stime);
                        double difff = entry.getValue() * stime - value;
                        double kl = Math.random();
                        if (kl < difff || value == 0) {
                            value++;
                        }

                        for (int k = 0; k < calDegrees.size(); k++) {
                            value = Math.min(value, avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)));
                            if (!coun.containsKey(avaAttributes.get(k))) {
                                coun.put(avaAttributes.get(k), new ArrayList<ArrayList<Integer>>());
                            }
                            coun.get(avaAttributes.get(k)).add(calDegrees.get(k));
                        }
//System.out.println(value);
                        if (value >= 1) {
                            if (value >= 1) {
                                int bound1 = this.allBounds.get(calDegrees);

                                if (!result.containsKey(calDegrees)) {
                                    result.put(calDegrees, 0);
                                }

                                if (result.get(calDegrees) + value >= bound1) {
                                    this.allBounds.remove(calDegrees);
                                    value = bound1 - result.get(calDegrees);
                                }

                                if (bound1 <= 0) {
                                    System.out.println("bound1: " + bound1);
                                }
                                //  System.out.println("value:" + value + "entry:" + entry+" bound:"+bound1);
                                if (value <= 0) {
                                    value = 0;
                                    this.boundTrash.add(calDegrees);
                                }
                                if (value > 0) {
                                    this.posValue = true;
                                }
                                result.put(calDegrees, value + result.get(calDegrees));

                                if (result.get(calDegrees) > bound1) {
                                    System.out.println("bound: " + bound1 + "  " + result.get(calDegrees));
                                }
                                for (int k = 0; k < calDegrees.size(); k++) {
                                    avaCounts.get(avaAttributes.get(k)).put(calDegrees.get(k), avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) - value);
                                }

                                for (int k = 0; k < calDegrees.size(); k++) {
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) < 0) {
                                        //   
                                        System.out.println("Errorr: " + value + " " + calDegrees + " " + avaAttributes.get(k));
                                    }
                                    if (avaCounts.get(avaAttributes.get(k)).containsKey(calDegrees.get(k)) && avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)) == 0) {
                                        Math.min(value, avaCounts.get(avaAttributes.get(k)).remove(calDegrees.get(k)));
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    private void thirdRound2(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround");

        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        //ArrayList<Double> avaSizeCounts = new ArrayList<>();
        int value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        for (int i = 0; i < avaAttributes.size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            arr = new ArrayList<>();
            arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());
            permutation.add(arr);
            value *= permutation.get(i).size();
        }
        int starter = 0;
        while (starter < value && permutation.get(0).size() > 0) {

            ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
            int temp = (int) starter;
            int tvalue = value;

            for (int i = 0; i < permutation.size(); i++) {
                int ind = temp / (tvalue / permutation.get(i).size());
                preVdegrees.add(permutation.get(i).get(ind));
                tvalue = tvalue / permutation.get(i).size();
                temp = temp % tvalue;
            }
            boolean found = false;

            boolean nega = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) - 1;
                avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);
                if (vv < 0 && (!eveNum || this.sumVector(preVdegrees.get(0)) < this.sumVector(preVdegrees.get(1)))) {
                    nega = true;
                    //     break;
                }
            }

            if (!nega && (!eveNum || this.sumVector(preVdegrees.get(0)) < this.sumVector(preVdegrees.get(1)))) {
                if (this.allBounds.containsKey(preVdegrees)) {
                    /*       int bound1=1;
                     if (this.allBounds.containsKey(preVdegrees)) {
                     bound1 = this.allBounds.get(preVdegrees);;
                     } else {
                     for (int t = 0; t < preVdegrees.size(); t++) {
                     bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(preVdegrees.get(t));
                     }
                     allBounds.put(preVdegrees, bound1);
                     }
                     */
                    if (con.containsKey(preVdegrees) && con.get(preVdegrees) < this.allBounds.get(preVdegrees)) {
                        con.put(preVdegrees, con.get(preVdegrees) + 1);
                        found = true;
                        starter--;
                    } else if (!con.containsKey(preVdegrees)) {
                        con.put(preVdegrees, 1);
                        found = true;
                        starter--;
                    }

                    if (con.get(preVdegrees) == this.allBounds.get(preVdegrees)) {
                        this.allBounds.remove(preVdegrees);
                        boundTrash.add(preVdegrees);
                    }
                }

            }

            if (!found) {
                for (int t = 0; t < preVdegrees.size(); t++) {
                    int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) + 1;
                    avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);

                }
            }

            boolean remov = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                if (avaCounts.get(avaAttributes.get(t)).containsKey(preVdegrees.get(t)) && avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) == 0) {
                    avaCounts.get(avaAttributes.get(t)).remove(preVdegrees.get(t));
                    remov = true;
                }
            }
            if (remov) {
                starter = -1;
                value = 1;
                permutation = new ArrayList<>();
                for (int i = 0; i < avaAttributes.size(); i++) {
                    ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
                    arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());

                    permutation.add(arr);

                    value *= permutation.get(i).size();
                }

            }
            starter++;
        }

        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
    }

    private void thirdRound(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround3");
//breaking the correlations
        int sums = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        //ArrayList<Double> avaSizeCounts = new ArrayList<>();
        int value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        for (int i = 0; i < avaAttributes.size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            arr = new ArrayList<>();
            arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());
            permutation.add(arr);
            value *= permutation.get(i).size();
        }
        int starter = 0;
        while (starter < value && permutation.get(0).size() > 0) {

            ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
            int temp = (int) starter;
            int tvalue = value;

            for (int i = 0; i < permutation.size(); i++) {
                int ind = temp / (tvalue / permutation.get(i).size());
                preVdegrees.add(permutation.get(i).get(ind));
                tvalue = tvalue / permutation.get(i).size();
                temp = temp % tvalue;
            }
            boolean found = false;

            boolean nega = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) - 1;
                avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);
                if (vv < 0) {
                    nega = true;
                    //     break;
                } else {
                    if ((eveNum && this.sumVector(preVdegrees.get(0)) > this.sumVector(preVdegrees.get(1)))) {
                        nega = true;
                    } else if (eveNum && this.sumVector(preVdegrees.get(0)) == this.sumVector(preVdegrees.get(1))) {
                        if (preVdegrees.get(0).equals(preVdegrees.get(1)) && preVdegrees.get(0).get(2) == 1) {

                        } else {
                            nega = true;
                        }
                    }
                }
            }

            if (!nega) {
                if (!this.boundTrash.contains(preVdegrees)) {
                    int bound1 = 1;
                    if (this.allBounds.containsKey(preVdegrees)) {
                        bound1 = this.allBounds.get(preVdegrees);;
                    } else {
                        for (int t = 0; t < preVdegrees.size(); t++) {
                            //    
                            if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(attributeOrders.get(t)).get(preVdegrees.get(t))) {
                                bound1 = Integer.MAX_VALUE;
                            } else {
                                bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(preVdegrees.get(t));
                            }
                            //      bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(preVdegrees.get(t));
                        }
                        allBounds.put(preVdegrees, bound1);
                    }
                    if (con.containsKey(preVdegrees) && con.get(preVdegrees) < this.allBounds.get(preVdegrees)) {
                        con.put(preVdegrees, con.get(preVdegrees) + 1);
                        found = true;
                        starter--;
                    } else if (!con.containsKey(preVdegrees)) {
                        con.put(preVdegrees, 1);
                        found = true;
                        starter--;
                    }
                    sums++;

                    if (con.get(preVdegrees) == this.allBounds.get(preVdegrees)) {
                        this.allBounds.remove(preVdegrees);
                        boundTrash.add(preVdegrees);
                    }
                }

            }

            if (!found) {
                for (int t = 0; t < preVdegrees.size(); t++) {
                    int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) + 1;
                    avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);

                }
            }

            boolean remov = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                if (avaCounts.get(avaAttributes.get(t)).containsKey(preVdegrees.get(t)) && avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) == 0) {
                    avaCounts.get(avaAttributes.get(t)).remove(preVdegrees.get(t));
                    remov = true;
                }
            }
            if (remov) {
                starter = -1;
                value = 1;
                permutation = new ArrayList<>();
                for (int i = 0; i < avaAttributes.size(); i++) {
                    ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
                    arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());

                    permutation.add(arr);

                    value *= permutation.get(i).size();
                }

            }
            starter++;
        }

        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        System.out.println("sumn:" + sums);
    }

    private void lastRound(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround");
        int sumn = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        //ArrayList<Double> avaSizeCounts = new ArrayList<>();
        int value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        for (int i = 0; i < avaAttributes.size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            arr = new ArrayList<>();
            arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());
            permutation.add(arr);
            value *= permutation.get(i).size();
        }
        int starter = 0;

        while (starter < value && avaCounts.get(avaAttributes.get(0)).size() > 0) {
            //    System.out.println("=========="+starter +"value: "+value);
            if (eveNum && Math.random() > 0.99) {
                System.out.println(avaCounts);
            }
            ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
            int temp = (int) starter;
            int tvalue = value;
            for (int i = 0; i < permutation.size(); i++) {
                int ind = temp / (tvalue / permutation.get(i).size());
                preVdegrees.add(permutation.get(i).get(ind));
                tvalue = tvalue / permutation.get(i).size();
                temp = temp % tvalue;
            }
            boolean found = false;

            boolean nega = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) - 1;
                avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);
                if (vv < 0) {
                    nega = true;
                } else {
                    if (eveNum && this.sumVector(preVdegrees.get(0)) >= this.sumVector(preVdegrees.get(1))) {
                        nega = true;
                    }
                }
            }

            if (!nega & !found) {
                for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
                    //           if (entry.getValue() >= 1 && !found && (!eveNum || this.sumVector(entry.getKey().get(0)) < this.sumVector(entry.getKey().get(1)))) {
                    if (entry.getValue() >= 1 && !found) {
                        for (int k = 0; k < avaAttributes.size(); k++) {
                           // System.out.println("kkk");

                            ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
                            ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();
                            for (int i = 0; i < k; i++) {
                                pair1.add(preVdegrees.get(i));
                                pair2.add(entry.getKey().get(i));
                            }
                            pair2.add(preVdegrees.get(k));
                            pair1.add(entry.getKey().get(k));

                            for (int i = k + 1; i < avaAttributes.size(); i++) {
                                pair1.add(preVdegrees.get(i));
                                pair2.add(entry.getKey().get(i));
                            }
                            int bound1 = 1, bound2 = 1;
                            if (!this.boundTrash.contains(pair1) && !this.boundTrash.contains(pair2)) {
                                if (this.allBounds.containsKey(pair1) && this.allBounds.containsKey(pair2)) {
                                    bound1 = this.allBounds.get(pair1);
                                    bound2 = this.allBounds.get(pair2);
                                } else {
                                    for (int t = 0; t < pair1.size(); t++) {
                                        bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(pair1.get(t));
                                        bound2 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(pair2.get(t));
                                    }
                                    allBounds.put(pair2, bound2);
                                    allBounds.put(pair1, bound1);
                                }
                                boolean fla = false;
//System.out.println(bound1+" bound2: "+bound2);
                                if (eveNum) {
                                    if (this.sumVector(pair1.get(0)) <= this.sumVector(pair1.get(1)) && this.sumVector(pair2.get(0)) <= this.sumVector(pair2.get(1))) {
                                        fla = true;
                                    }
                                }
                                if ((!con.containsKey(pair1) || con.get(pair1) < bound1) && (!con.containsKey(pair2) || con.get(pair2) < bound2) && (!eveNum || (fla && (!pair1.equals(pair2) && !entry.getKey().equals(pair1) && !entry.getKey().equals(pair2))))) {
                             //    if (true){
                                    //       System.out.println("kkkkkk");
                                    con.put(entry.getKey(), entry.getValue() - 1);
                                    if (!con.containsKey(pair1)) {
                                        con.put(pair1, 0);
                                    }
                                    if (!con.containsKey(pair2)) {
                                        con.put(pair2, 0);
                                    }
                                    //      System.out.println("starter: "+starter);
                                    con.put(pair1, 1 + con.get(pair1));
                                    con.put(pair2, 1 + con.get(pair2));
                                    starter--;
                                    sumn++;
                                    if (con.get(pair1) == allBounds.get(pair1)) {
                                        allBounds.remove(pair1);
                                        this.boundTrash.add(pair1);
                                    }
                                    if (con.get(pair2) == allBounds.get(pair2)) {
                                        allBounds.remove(pair2);
                                        this.boundTrash.add(pair2);
                                    }
                                    if (bound1 < con.get(pair1)) {
                                        System.out.println("pair1: " + pair1 + "    " + bound1 + "  " + con.get(pair1));
                                    }
                                    if (bound2 < con.get(pair2)) {
                                        System.out.println("pair2: " + pair2 + "    " + bound2 + "  " + con.get(pair2));
                                    }
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                }

            }

            if (!found) {
                for (int t = 0; t < preVdegrees.size(); t++) {
                    int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) + 1;
                    avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);

                }
            } else {

                boolean remov = false;
                for (int t = 0; t < preVdegrees.size(); t++) {
                    if (avaCounts.get(avaAttributes.get(t)).containsKey(preVdegrees.get(t)) && avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) == 0) {
                        avaCounts.get(avaAttributes.get(t)).remove(preVdegrees.get(t));
                        remov = true;
                    }
                }
                if (remov) {
                    starter = -1;
                    value = 1;
                    permutation = new ArrayList<>();
                    for (int i = 0; i < avaAttributes.size(); i++) {
                        ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
                        arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());

                        permutation.add(arr);

                        value *= arr.size();
                    }
                    //      System.out.println("value: "+value);
                    if (value == 1) {
                        System.out.println(avaCounts);
                    }

                }
            }
            starter++;
        }

        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        System.out.println("sums:" + sumn);
    }

    private void lastRoundOld(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround");

        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
            con.put(entry.getKey(), entry.getValue());
        }
        ArrayList<Double> avaSizeCounts = new ArrayList<>();
        double value = 1;
        ArrayList<ArrayList<ArrayList<Integer>>> permutation = new ArrayList<>();

        //  System.out.println(avaAttributes);
        for (int i = 0; i < avaAttributes.size(); i++) {
            ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
            arr = new ArrayList<>();
            avaSizeCounts.add(avaCounts.get(avaAttributes.get(i)).keySet().size() * 1.0);
            value *= avaSizeCounts.get(i);
            arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());
            permutation.add(arr);
        }

        for (int i = avaSizeCounts.size() - 2; i >= 0; i--) {
            avaSizeCounts.set(i, avaSizeCounts.get(i) * avaSizeCounts.get(i + 1));
        }
        //      while (starter<)
        double starter = 0;
        while (avaSizeCounts.size() > 0 && starter < avaSizeCounts.get(0)) {
            if (eveNum && Math.random() > 0.99) {
                System.out.println(avaCounts);
            }
            ArrayList<Integer> indexes = new ArrayList<>();
            ArrayList<ArrayList<Integer>> preVdegrees = new ArrayList<>();
            double temp = starter;
            for (int i = 1; i < avaSizeCounts.size(); i++) {
                indexes.add((int) (temp / avaSizeCounts.get(i)));
                preVdegrees.add(permutation.get(i - 1).get((int) (temp / avaSizeCounts.get(i))));
                temp = temp % avaSizeCounts.get(i);
            }

            preVdegrees.add(permutation.get(avaSizeCounts.size() - 1).get((int) temp));
            boolean nega = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) - 1;
                avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);
                if (vv < 0 && (!eveNum || this.sumVector(preVdegrees.get(0)) <= this.sumVector(preVdegrees.get(1)))) {
                    nega = true;
                    //     break;
                }
            }

            boolean found = false;
            if (!nega) {
                for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
                    if (entry.getValue() >= 1 && !found && (this.sumVector(entry.getKey().get(0)) <= this.sumVector(entry.getKey().get(1)) || !eveNum)) {
                        for (int k = 0; k < avaAttributes.size(); k++) {
                            ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
                            ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();
                            for (int i = 0; i < k; i++) {
                                pair1.add(preVdegrees.get(i));
                                pair2.add(entry.getKey().get(i));
                            }
                            pair2.add(preVdegrees.get(k));
                            pair1.add(entry.getKey().get(k));

                            for (int i = k + 1; i < avaAttributes.size(); i++) {
                                pair1.add(preVdegrees.get(i));
                                pair2.add(entry.getKey().get(i));
                            }
                            int bound1 = 1, bound2 = 1;
                            if (this.allBounds.containsKey(pair1) && this.allBounds.containsKey(pair2) || true) {
                                //  bound1 = this.allBounds.get(pair1);
                                //  bound2 = this.allBounds.get(pair2);

                                for (int t = 0; t < pair1.size(); t++) {
                                    bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(pair1.get(t));
                                    bound2 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(pair2.get(t));
                                }
                                boolean fla = false;

                                if (eveNum) {
                                    //    System.out.println("starter: " + starter);
                                    if (this.sumVector(pair1.get(0)) < this.sumVector(pair1.get(1)) && this.sumVector(pair2.get(0)) < this.sumVector(pair2.get(1))) {
                                        fla = true;
                                    }
                                }
                                if ((!con.containsKey(pair1) || con.get(pair1) < bound1) && (!con.containsKey(pair2) || con.get(pair2) < bound2) && !pair1.equals(pair2) && !entry.getKey().equals(pair1) && !entry.getKey().equals(pair2) && (!eveNum || fla)) {
                                    con.put(entry.getKey(), entry.getValue() - 1);
                                    if (!con.containsKey(pair1)) {
                                        con.put(pair1, 0);
                                    }
                                    if (!con.containsKey(pair2)) {
                                        con.put(pair2, 0);
                                    }
                                    con.put(pair1, 1 + con.get(pair1));
                                    con.put(pair2, 1 + con.get(pair2));
                                    starter--;
                                    if (bound1 < con.get(pair1)) {
                                        System.out.println("pair1: " + pair1 + "    " + bound1 + "  " + con.get(pair1));
                                    }
                                    if (bound2 < con.get(pair2)) {
                                        System.out.println("pair2: " + pair2 + "    " + bound2 + "  " + con.get(pair2));
                                    }
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                }

            }
            if (!found) {
                for (int t = 0; t < preVdegrees.size(); t++) {
                    int vv = avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) + 1;
                    avaCounts.get(avaAttributes.get(t)).put(preVdegrees.get(t), vv);

                }
            }

            boolean remov = false;
            for (int t = 0; t < preVdegrees.size(); t++) {
                if (avaCounts.get(avaAttributes.get(t)).containsKey(preVdegrees.get(t)) && avaCounts.get(avaAttributes.get(t)).get(preVdegrees.get(t)) == 0) {
                    avaCounts.get(avaAttributes.get(t)).remove(preVdegrees.get(t));
                    remov = true;
                }
            }
            if (remov) {
                starter = -1;
                avaSizeCounts = new ArrayList<>();
                value = 1;
                permutation = new ArrayList<>();
                for (int i = 0; i < avaAttributes.size(); i++) {
                    ArrayList<ArrayList<Integer>> arr = new ArrayList<>();
                    avaSizeCounts.add(avaCounts.get(avaAttributes.get(i)).keySet().size() * 1.0);
                    value *= avaSizeCounts.get(i);
                    arr.addAll(avaCounts.get(avaAttributes.get(i)).keySet());
                    permutation.add(arr);
                }
                for (int i = avaSizeCounts.size() - 2; i >= 0; i--) {
                    avaSizeCounts.set(i, avaSizeCounts.get(i) * avaSizeCounts.get(i + 1));
                }
            }
            starter++;
        }

        for (Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
    }

    private void processDistanceMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts) {
        for (Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry : avaCounts.entrySet()) {
            this.distanceMap.put(entry.getKey(), this.closestMap(entry.getValue().keySet()));
        }
    }

}
