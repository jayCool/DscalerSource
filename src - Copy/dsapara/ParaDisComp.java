package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author workshop
 */
public class ParaDisComp implements Runnable {
HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap;
Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry;
boolean stop=false;
    ParaDisComp(HashMap<ArrayList<String>, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap, Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry) {
        this.distanceMap = distanceMap;
        this.entry =entry;
    }

    @Override
    public void run() {
           this.distanceMap.put(entry.getKey(), this.closestMap(entry.getValue().keySet())); 
           stop=true;
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
}
