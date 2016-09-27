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
public class ParaCorrMap implements Runnable {

    boolean eveNum = true;
    ArrayList<Integer> curIndexes;
    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts;
    HashMap<ArrayList<ArrayList<Integer>>, Integer> evalue;
    ArrayList<ArrayList<String>> attributeOrders;
    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution;
    ArrayList<ArrayList<String>> avaAttributes;
    HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio;
    HashMap<String, ArrayList<String>> mergedDegreeTitle;
    ArrayList<String> ekey;
    HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result;
    List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> sorted;

    boolean posValue = false;
    HashSet<ArrayList<ArrayList<Integer>>> boundTrash = new HashSet<>();
    HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap = new HashMap<>();
    HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();
    boolean stop = false;

    ParaCorrMap(HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap, ArrayList<Integer> curIndexes, HashMap<ArrayList<String>, HashMap<ArrayList<ArrayList<Integer>>, Integer>> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> value, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<String, ArrayList<String>> mergedDegreeTitle, ArrayList<String> key) {
        this.avaCounts = avaCounts;
        this.evalue = value;
        this.attributeOrders = attributeOrders;
        this.downsizedMergedDistribution = downsizedMergedDistribution;
        this.avaAttributes = avaAttributes;
        this.downsizedMergedRatio = downsizedMergedRatio;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.ekey = key;
        this.result = result;
        this.curIndexes = curIndexes;
        this.distanceMap = distanceMap;
    }

    @Override
    public void run() {
        HashMap<ArrayList<ArrayList<Integer>>, Integer> corred = new HashMap<>();

        corred = produceCorrMap(avaCounts, evalue, attributeOrders, downsizedMergedDistribution, avaAttributes, downsizedMergedRatio, mergedDegreeTitle);
        result.put(ekey, corred);
        stop = true;

    }
    int iteration = 0;
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

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> mappingAllValueAesc(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        iteration++;
        System.out.println("startSorting");
System.out.println(avaCounts.keySet());
        for (int i = 0; !avaCounts.isEmpty() && i < sorted.size() && !avaCounts.get(avaAttributes.get(0)).isEmpty() &&!avaCounts.get(avaAttributes.get(0)).keySet().isEmpty() && sorted.size() > 0; i++) {
            Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry = sorted.get(i);
            ArrayList<ArrayList<Integer>> calDegrees = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<Integer>>> degreeSets = new ArrayList<>();
                int totalNum = 1;
                int startCount = 0;
                int curNum = startCount;
                this.posValue = false;
                int value = entry.getValue();
                for (int k = 0; k < entry.getKey().size(); k++) {
                    ArrayList<ArrayList<Integer>> calset = this.normMapCandidate(distanceMap.get(avaAttributes.get(k)), entry.getKey().get(k));

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
                    value = (int) Math.ceil(entry.getValue() * rat);
                    
                    //value = (int) (entry.getValue() * rat);
                    if (value <= 0) {
                        value++;
                    }

                    startCount++;
                    if (!this.boundTrash.contains(calDegrees) && (!eveNum || (eveNum && (this.sumVector(calDegrees.get(0)) < this.sumVector(calDegrees.get(1)) || (calDegrees.get(0) == calDegrees.get(1) && calDegrees.get(0).get(2) == 1))))) {
                        updateValue(calDegrees, avaCounts, attributeOrders, downsizedMergedDistribution, avaAttributes, result, value);
                    }
                }
          
            //   }
        }
        return result;
    }
    int totalSum = 0;

    private void updateValue(ArrayList<ArrayList<Integer>> calDegrees, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, int value) {
      //  if (Math.random() > 0.999) {
       //     System.out.println(attributeOrders + " " + this.totalSum);
      //  }
        for (int k = 0; k < calDegrees.size(); k++) {

            value = Math.min(value, avaCounts.get(avaAttributes.get(k)).get(calDegrees.get(k)));
        }
        if (value >= 1) {
            int bound1 = 1;
            if (this.allBounds.containsKey(calDegrees)) {
                bound1 = this.allBounds.get(calDegrees);;
            } else {
                if (calDegrees.size() == 1) {
                    bound1 = avaCounts.get(avaAttributes.get(0)).get(calDegrees.get(0));
                } else {
                    for (int t = 0; t < calDegrees.size(); t++) {

                        if (bound1 >= Integer.MAX_VALUE / downsizedMergedDistribution.get(attributeOrders.get(t)).get(calDegrees.get(t))) {
                            bound1 = Integer.MAX_VALUE;
                        } else {
                            bound1 *= downsizedMergedDistribution.get(attributeOrders.get(t)).get(calDegrees.get(t));
                        }
                    }
                //only one FK

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

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> produceCorrMap(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> correlationFunction, ArrayList<ArrayList<String>> attributeOrders, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> downsizedMergedRatio, HashMap<String, ArrayList<String>> mergedDegreeTitle) {
        iteration = 0;
        System.out.println("iteration: 0");
        this.boundTrash.clear();
        HashMap<ArrayList<ArrayList<Integer>>, Integer> result = new HashMap<>();
        allBounds = new HashMap<>();
        this.totalSum = 0;
        //   System.out.println(avaCounts);
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

    private void thirdRound(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround3");
//breaking the correlations
        int sums = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
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

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        System.out.println("sumn:" + sums);
    }

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    private void lastRound(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts, HashMap<ArrayList<ArrayList<Integer>>, Integer> result, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> downsizedMergedDistribution, ArrayList<ArrayList<String>> avaAttributes, ArrayList<ArrayList<String>> attributeOrders) {
        System.out.println("lastround");
        int sumn = 0;
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : result.entrySet()) {
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
                //    System.out.println(avaCounts);
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
                for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
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
                    //        if (value==1) System.out.println(avaCounts);

                }
            }
            starter++;
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : con.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        System.out.println("sums:" + sumn);
    }

}
