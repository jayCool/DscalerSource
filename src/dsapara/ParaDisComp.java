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
HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap;
Map.Entry<String, HashMap<ArrayList<Integer>, AvaStat>> entry;
ComKey ck;
boolean stop=false;
    ParaDisComp(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> distanceMap, 
            Map.Entry<String, HashMap<ArrayList<Integer>, AvaStat>> entry,ComKey ck) {
        this.distanceMap = distanceMap;
        this.entry =entry;
        this.ck = ck;
    }
    @Override
    public void run() {
            System.out.println(ck);
            HashMap<Integer, ArrayList<ArrayList<Integer>>> temp = this.closestMap(entry.getValue().keySet());
           this.distanceMap.put(ck,temp ); 
           stop=true;
    }
    
    public void singlerun() {
            System.out.println(ck);
            HashMap<Integer, ArrayList<ArrayList<Integer>>> temp = this.closestMap(entry.getValue().keySet());
           this.distanceMap.put(ck,temp ); 
           stop=true;
    }
    
      private HashMap<Integer, ArrayList<ArrayList<Integer>>> closestMap(Set<ArrayList<Integer>> keySet) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> map = new HashMap<>();
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
