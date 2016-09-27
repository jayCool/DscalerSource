/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author workshop
 */
public class LocalRefTableGen implements Runnable {

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseDistribution;
    HashMap<String, HashMap<Integer, Integer>> result;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> entry;
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> mapping = new HashMap<>();

    @Override
    public void run() {
        sortDis(reverseDistribution.get(entry.getKey()).keySet());
        ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
        HashMap<Integer, Integer> mmap = new HashMap<>();
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> tempH = new ConcurrentHashMap<>();
        int total = 0;
        for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
            ArrayList<Integer> arr = new ArrayList<>();
            for (int t : entry2.getValue()) {
                arr.add(t);
                total++;
            }
            if (arr.size() > 0) {
                tempH.put(entry2.getKey(), arr);
            }
        }

        int level = 0;
        int sum = 0;
        System.out.println(entry.getKey());
        while (tempH.keySet().size() > 0) {

            for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry2 : tempH.entrySet()) {
                int leftOver = entry2.getValue().size();
                if (leftOver == 0) {
                    tempH.remove(entry2.getKey());
                    continue;
                }
                if (level == 0) {
                    calDeg = entry2.getKey();
                    if (reverseDistribution.get(entry.getKey()).containsKey(calDeg) && reverseDistribution.get(entry.getKey()).get(calDeg).size() > 0) {
                        int size = reverseDistribution.get(entry.getKey()).get(calDeg).size();
                        ArrayList<Integer> oldIDs = new ArrayList<>();
                        for (int i : reverseDistribution.get(entry.getKey()).get(calDeg)) {
                            oldIDs.add(i);
                        }
                        Collections.shuffle(oldIDs);
                        for (int i = 0; i < leftOver; i++) {
                            mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                        }
                        sum += leftOver;
                        tempH.remove(entry2.getKey());
                    }
                } else {
                   
                    if (reverseDistribution.get(entry.getKey()).containsKey(entry2.getKey())) {
                        calDeg = entry2.getKey();
                    } else {
                        calDeg = nearestSum(entry2.getKey());
                    }

                    boolean check = false;
                    while (reverseDistribution.get(entry.getKey()).get(calDeg).size() == 0) {
                        reverseDistribution.get(entry.getKey()).remove(calDeg);
                        int sum1 = 0;
                        sum1 = getSum(calDeg);
                        mapping.get(sum1).remove(calDeg);
                        if (mapping.get(sum1).size() == 0) {
                            mapping.remove(sum1);
                        }
                        check = true;
                        break;
               
                    }
                    if (check) {
                        continue;
                    }
                    int size = reverseDistribution.get(entry.getKey()).get(calDeg).size();

                    ArrayList<Integer> oldIDs = new ArrayList<>();
                    for (int i : reverseDistribution.get(entry.getKey()).get(calDeg)) {
                        oldIDs.add(i);
                    }
                    Collections.shuffle(oldIDs);
                    for (int i = 0; i < leftOver; i++) {
                        mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                    }
                    sum += leftOver;
                    tempH.remove(entry2.getKey());
                }
            }
            level++;
        }
        System.out.println(entry.getKey() + " DONE  " + sum);
        reverseDistribution.remove(entry.getKey());
        result.put(entry.getKey(), mmap);
    }

    private ArrayList<ArrayList<ArrayList<Integer>>> l1Norm2Sets(ArrayList<ArrayList<Integer>> key, Set<ArrayList<ArrayList<Integer>>> keySet) {
        int maxABS = 0;
        ArrayList<ArrayList<ArrayList<Integer>>> result = new ArrayList<>();
        HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> map = new HashMap<>();
        if (keySet.contains(key)) {
            result.add(key);
            return result;
        }
        for (ArrayList<ArrayList<Integer>> arr : keySet) {
            int diff = 0;
            for (int i = 0; i < key.size() && arr.size() >= key.size(); i++) {
                for (int j = 0; j < key.get(i).size() && key.get(i).size() >= arr.get(i).size(); j++) {
                    diff += Math.abs(key.get(i).get(j) - arr.get(i).get(j));
                }
            }
            maxABS = Math.max(maxABS, Math.abs(diff));

            if (!map.containsKey(Math.abs(diff))) {
                map.put(Math.abs(diff), new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            map.get(Math.abs(diff)).add(arr);
            if (diff == 1) {
                break;
            }
        }

        //  System.out.println(map.size());
        for (int i = 0; i <= maxABS; i++) {
            if (map.containsKey(i)) {
                result.addAll(map.get(i));
                break;
            }
        }
        return result;
    }

    private void sortDis(Set<ArrayList<ArrayList<Integer>>> keySet) {

        for (ArrayList<ArrayList<Integer>> arr : keySet) {
            int sum = 0;
            for (ArrayList<Integer> karr : arr) {
                for (int i : karr) {
                    sum += i;
                }
            }
            if (!mapping.containsKey(sum)) {
                mapping.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            mapping.get(sum).add(arr);
        }

    }

    private ArrayList<ArrayList<Integer>> nearestSum(ArrayList<ArrayList<Integer>> key) {
        int sum1 = getSum(key);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (mapping.containsKey(sum1 + i)) {
                int size = mapping.get(sum1 + i).size();
                if (size == 0) {
                    mapping.remove(sum1 + i);
                } else {
                    int can = (int) (Math.random() * (size - 1) + 0.45);
                    return mapping.get(sum1 + i).get(can);
                }
            }
            if (mapping.containsKey(sum1 - i)) {
                int size = mapping.get(sum1 - i).size();
                int can = (int) (Math.random() * (size - 1) + 0.45);
                return mapping.get(sum1 - i).get(can);
            }
        }
        return null;
    }

    private int getSum(ArrayList<ArrayList<Integer>> calDeg) {
        int sum1 = 0;
        for (ArrayList<Integer> ks : calDeg) {
            for (int i : ks) {
                sum1 += i;
            }
        }
        return sum1;
    }
}
