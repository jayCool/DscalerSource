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

    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseKV;
    HashMap<String, HashMap<Integer, Integer>> result;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> kvIDentry;
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> sum2KVmap = new HashMap<>();

    @Override
    public void run() {
        sortDis(reverseKV.get(kvIDentry.getKey()).keySet());
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> conKVIDEntry = createConCurrent();
      
        HashMap<Integer, Integer> mmap = new HashMap<>();
       ArrayList<ArrayList<Integer>> calDeg = new ArrayList<>();
        
        int level = 0;
        int sum = 0;
        //System.out.println(kvIDentry.getKey());
        while (conKVIDEntry.keySet().size() > 0) {

            for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry2 : conKVIDEntry.entrySet()) {
                int leftOver = entry2.getValue().size();
                if (leftOver == 0) {
                    conKVIDEntry.remove(entry2.getKey());
                    continue;
                }
                if (level == 0) {
                    calDeg = entry2.getKey();
                    if (reverseKV.get(kvIDentry.getKey()).containsKey(calDeg) && reverseKV.get(kvIDentry.getKey()).get(calDeg).size() > 0) {
                        int size = reverseKV.get(kvIDentry.getKey()).get(calDeg).size();
                        ArrayList<Integer> oldIDs = new ArrayList<>();
                        for (int i : reverseKV.get(kvIDentry.getKey()).get(calDeg)) {
                            oldIDs.add(i);
                        }
                        Collections.shuffle(oldIDs);
                        for (int i = 0; i < leftOver; i++) {
                            mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                        }
                        sum += leftOver;
                        conKVIDEntry.remove(entry2.getKey());
                    }
                } else {
                   
                    if (reverseKV.get(kvIDentry.getKey()).containsKey(entry2.getKey())) {
                        calDeg = entry2.getKey();
                    } else {
                        calDeg = nearestSum(entry2.getKey());
                    }

                    boolean check = false;
                    while (reverseKV.get(kvIDentry.getKey()).get(calDeg).size() == 0) {
                        reverseKV.get(kvIDentry.getKey()).remove(calDeg);
                        int sum1 = 0;
                        sum1 = getSum(calDeg);
                        sum2KVmap.get(sum1).remove(calDeg);
                        if (sum2KVmap.get(sum1).size() == 0) {
                            sum2KVmap.remove(sum1);
                        }
                        check = true;
                        break;
               
                    }
                    if (check) {
                        continue;
                    }
                    int size = reverseKV.get(kvIDentry.getKey()).get(calDeg).size();

                    ArrayList<Integer> oldIDs = new ArrayList<>();
                    for (int i : reverseKV.get(kvIDentry.getKey()).get(calDeg)) {
                        oldIDs.add(i);
                    }
                    Collections.shuffle(oldIDs);
                    for (int i = 0; i < leftOver; i++) {
                        mmap.put(entry2.getValue().get(i), oldIDs.get(i % size));
                    }
                    sum += leftOver;
                    conKVIDEntry.remove(entry2.getKey());
                }
            }
            level++;
        }
        System.out.println(kvIDentry.getKey() + " DONE  " + sum);
        reverseKV.remove(kvIDentry.getKey());
        result.put(kvIDentry.getKey(), mmap);
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

    private void sortDis(Set<ArrayList<ArrayList<Integer>>> kvSet) {

        for (ArrayList<ArrayList<Integer>> kv : kvSet) {
            int sum = 0;
            for (ArrayList<Integer> jointDegree : kv) {
                for (int degree : jointDegree) {
                    sum += degree;
                }
            }
            if (!sum2KVmap.containsKey(sum)) {
                sum2KVmap.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            sum2KVmap.get(sum).add(kv);
        }

    }

    private ArrayList<ArrayList<Integer>> nearestSum(ArrayList<ArrayList<Integer>> key) {
        int sum1 = getSum(key);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (sum2KVmap.containsKey(sum1 + i)) {
                int size = sum2KVmap.get(sum1 + i).size();
                if (size == 0) {
                    sum2KVmap.remove(sum1 + i);
                } else {
                    int can = (int) (Math.random() * (size - 1) + 0.45);
                    return sum2KVmap.get(sum1 + i).get(can);
                }
            }
            if (sum2KVmap.containsKey(sum1 - i)) {
                int size = sum2KVmap.get(sum1 - i).size();
                int can = (int) (Math.random() * (size - 1) + 0.45);
                return sum2KVmap.get(sum1 - i).get(can);
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

    private ConcurrentHashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> createConCurrent() {
      ConcurrentHashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> conKVIDEntry = new ConcurrentHashMap<>();
       
        for (Map.Entry<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> entry : kvIDentry.getValue().entrySet()) {
            ArrayList<Integer> idList = new ArrayList<>();
            for (int t : entry.getValue()) {
                idList.add(t);
            }
            if (idList.size() > 0) {
                conKVIDEntry.put(entry.getKey(), idList);
            }
        }
    return conKVIDEntry;
    }
}
