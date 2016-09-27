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
public class ParaProcessAvaID  implements Runnable {
int i;
  HashMap<ArrayList<Integer>, ArrayList<Integer>> aar = new HashMap<>();

Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry;
 HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree;
  HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> result ;boolean stop=false;
   HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts;
    ParaProcessAvaID(HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> result, HashMap<ArrayList<Integer>, ArrayList<Integer>> aar, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> updatedDegree, int i, Map.Entry<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> entry, HashMap<ArrayList<String>, HashMap<ArrayList<Integer>, Integer>> avaCounts) {
        this.result=result;
        this.aar = aar;
        this.i=i;
        this.updatedDegree = updatedDegree;
        this.entry = entry;
        this.avaCounts=avaCounts;
    }
      
    @Override
    public void run() {
              int startID = 1;
                ArrayList<String> keys = new ArrayList<>();
                keys.add(entry.getKey().get(0));
                keys.add(entry.getKey().get(i));
         HashMap<ArrayList<Integer>, HashMap<Integer, Integer>> ava = new HashMap<>();
//
         
          HashMap<ArrayList<Integer>, Integer> avadeg = new HashMap<>();
     //     
                for (Map.Entry<ArrayList<Integer>, Integer> entry2 : entry.getValue().entrySet()) {
                    HashMap<Integer, Integer> queue = new HashMap<>();
                    int repetNum = entry2.getKey().get(i - 1);
                     int v=entry2.getValue() * entry2.getKey().get(i - 1);
                    if (v > 0) {
                        avadeg.put(entry2.getKey(), v);
                    }
                    
                    if (i==1&&!aar.containsKey(entry2.getKey())) {
                        aar.put(entry2.getKey(), new ArrayList<Integer>());
                    }
                    for (int j = 1; j <= entry2.getValue(); j++) {
                        if (i == 1) {
                            aar.get(entry2.getKey()).add(j + startID);
                        }
                        queue.put(j + startID, repetNum);
                    }
                    startID += entry2.getValue();
                    ava.put(entry2.getKey(), queue);
                }
                              avaCounts.put(keys, avadeg);
 
                
                if (i == 1) {
                    updatedDegree.put(entry.getKey(), aar);
                }
                result.put(keys, ava); 
    stop=true;}
    
}
