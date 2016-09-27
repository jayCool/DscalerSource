/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 *
 * @author workshop
 */
public class ParaSort implements Runnable {

    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry;
    HashMap<String, List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>>> sortedCorrs;
    HashMap<String, ArrayList<ComKey>> referencingTable;

    private int sumVector(ArrayList<Integer> x) {
        int sum = 0;
        for (int y : x) {
            sum += y;
        }
        return sum;
    }

    @Override
    public void run() {
        Sort s = new Sort();
        // System.out.println(entry.getKey());
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> list = s.sortOnValueAESC(entry.getValue());
        //s.sortOnKeyPosition(entry.getValue(), curIndexes);
        List<Map.Entry<ArrayList<ArrayList<Integer>>, Integer>> list2 = new ArrayList<>();
        if (entry.getKey().equals("socialgraph")) {
            int start = 0;
            HashSet<Integer> idArr = new HashSet<>();
            while (start < list.size()) {
                Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 = list.get(start);
                if (this.sumVector(entry2.getKey().get(0)) > this.sumVector(entry2.getKey().get(1))) //  list.remove(start);
                {
                    idArr.add(start);
                }
                start++;
             }

            start = 0;
            while (start < list.size()) {
                if (!idArr.contains(start)) {
                    list2.add(list.get(start));
                }
                start++;
            }
            System.out.println(list2.size());
            list = list2;
            System.out.print(list.size());
        }
        sortedCorrs.put(entry.getKey(), list);
    }

}
