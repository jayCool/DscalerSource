/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author workshop
 */
public class ParallelProcessRatio implements Runnable {
HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> result;
Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry;
 HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution;
    boolean stop=false;
    ParallelProcessRatio(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Double>> result, Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> mergedDistribution) {
        this.result = result;
        this.entry = entry;
        this.mergedDistribution = mergedDistribution;
    }

    @Override
    public void run() {
      HashMap<ArrayList<Integer>, Double> map = new HashMap<>();
            
            for (Map.Entry<ArrayList<Integer>, Integer> entry2 : entry.getValue().entrySet()) {
                if (!mergedDistribution.get(entry.getKey()).containsKey(entry2.getKey())) {
                    map.put(entry2.getKey(), 1.0);
                } else {
                    map.put(entry2.getKey(), 1.0 * entry2.getValue() / mergedDistribution.get(entry.getKey()).get(entry2.getKey()));
                }
            }
            result.put(entry.getKey(), map);
            stop=true;
    }
    
}
