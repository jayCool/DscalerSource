/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.ComKey;
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
public class GlobalJoinTableGen implements Runnable {

    Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> entry;
    HashMap<String, HashMap<Integer, String>> result;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<String>>> reverseMergedDegree;
    double s = 0.2;

    private ArrayList<ArrayList<Integer>> l1Norm2Sets(ArrayList<Integer> pair, Set<ArrayList<Integer>> keySet) {
        int maxABS = 0;

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();

        for (ArrayList<Integer> arr : keySet) {
            int diff = 0;
            for (int i = 0; i < pair.size() && arr.size() >= pair.size(); i++) {
                diff += Math.abs(pair.get(i) - arr.get(i));
            }
            maxABS = Math.max(maxABS, Math.abs(diff));

            if (!map.containsKey(Math.abs(diff))) {
                map.put(Math.abs(diff), new ArrayList<ArrayList<Integer>>());
            }
            map.get(Math.abs(diff)).add(arr);
        }

        //  System.out.println(map.size());
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i <= maxABS; i++) {
            if (map.containsKey(i)) {
                result.addAll(map.get(i));

            }
        }
        return result;

    }
    int leftOver = 0;
    int start = 0;
    ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> tempH;
  int sum = 0;
    @Override
    public void run() {
        ArrayList<ArrayList<Integer>> calDegs = new ArrayList<>();
        ArrayList<Integer> calDeg = new ArrayList<>();
        HashMap<String, Integer> rridFre = new HashMap<>();
        HashMap<Integer, String> mmap = new HashMap<>();

        //count the frequency of the appeared rrid    
        tempH = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : entry.getValue().entrySet()) {
            ArrayList<Integer> arr = new ArrayList<>();
            for (int t : entry2.getValue()) {
                arr.add(t);
            }
            tempH.put(entry2.getKey(), arr);
        }
        int level = 0;
        //   
        HashMap<ArrayList<Integer>, Integer> countH = new HashMap<>();
        //  HashMap<ArrayList<Integer>, Integer> countQ = new HashMap<>();
        HashMap<ArrayList<String>, Integer> countL = new HashMap<>();
        while (tempH.keySet().size() > 0) {

            for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> entry2 : tempH.entrySet()) {
                if (!countH.containsKey(entry2.getKey())) {
                    countH.put(entry2.getKey(), 0);
                }
                start = countH.get(entry2.getKey());
                leftOver = entry2.getValue().size() - start;
                //
                if (level == 0) {
                    calDeg = entry2.getKey();
                    if (reverseMergedDegree.get(entry.getKey()).containsKey(calDeg) && reverseMergedDegree.get(entry.getKey()).get(calDeg).size() > 0) {
                        produceValue(countL, countH, mmap, entry2, calDeg);
                    }
                } else {
                  //  System.out.println("Wrong");
                    calDegs = l1Norm2Sets(entry2.getKey(), reverseMergedDegree.get(entry.getKey()).keySet());
                    while (leftOver > 0) {
                        calDeg = calDegs.remove(0);
                        while (reverseMergedDegree.get(entry.getKey()).get(calDeg).isEmpty()) {
                            reverseMergedDegree.get(entry.getKey()).remove(calDeg);
                            calDeg = calDegs.remove(0);
                        }

                        produceValue(countL, countH, mmap, entry2, calDeg);
                    }

                    tempH.remove(entry2.getKey());
                }

            }
            level++;
        }
        result.put(entry.getKey().get(0).sourceTable, mmap);
    }

    private void produceValue(HashMap<ArrayList<String>, Integer> countL, HashMap<ArrayList<Integer>, Integer> countH, HashMap<Integer, String> mmap, Map.Entry<ArrayList<Integer>, ArrayList<Integer>> entry2, ArrayList<Integer> calDeg) {
        ArrayList<String> oldIDs = new ArrayList<>();
        for (String i : reverseMergedDegree.get(entry.getKey()).get(calDeg)) {
            oldIDs.add(i);
        }

        if (!countL.containsKey(oldIDs)) {
            countL.put(oldIDs, (int) (oldIDs.size() * Math.ceil(this.s)));
        }
        int tempV = countL.get(oldIDs);
        int minV = Math.min(tempV, leftOver);
        int q = (int) (oldIDs.size() * Math.ceil(this.s)) - tempV;

        for (int i = 0; i < minV; i++) {

            q = q % oldIDs.size();
            String rrid = oldIDs.get(q);
            mmap.put(entry2.getValue().get(start), rrid);
            q++;
            start++;

        }
        countL.put(oldIDs, tempV - minV);
        if (countL.get(oldIDs) == 0) {
            reverseMergedDegree.get(entry.getKey()).remove(calDeg);
        }
        sum+=minV;
        leftOver = leftOver - minV;
        //  System.out.println(leftOver);
        if (leftOver == 0) {
            tempH.remove(entry2.getKey());
        } else {
            countH.put(entry2.getKey(), start);
        }
    }

}
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
