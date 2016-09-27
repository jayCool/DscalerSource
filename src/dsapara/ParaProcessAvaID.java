/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 *
 * @author workshop
 */
public class ParaProcessAvaID implements Runnable {

    int i;
    Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> entry;
    HashMap<ComKey, HashMap<ArrayList<Integer>, HashMap<Integer, Integer>>> result;
    boolean stop = false;
    HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo;
    HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts;

    ParaProcessAvaID(int i,
            Map.Entry<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> entry,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo, HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> avaCounts
    ) {
        this.i = i;
        this.entry = entry;
        this.avaInfo = avaInfo;
        this.avaCounts = avaCounts;

    }

    @Override
    public void run() {
        System.out.println(entry.getKey());

        int startID = 0;
        HashMap<ArrayList<Integer>, AvaStat> avadeg = new HashMap<>();
        for (int i = 0; i < entry.getKey().size(); i++) {
            avaCounts.put(entry.getKey().get(i), new HashMap<ArrayList<Integer>, Integer>());
        }
        for (Map.Entry<ArrayList<Integer>, Integer> entry2 : entry.getValue().entrySet()) {
           
            int[] startid = new int[entry.getKey().size()];
            int[] ids = new int[entry2.getValue()];

            for (int j = 0; j < entry2.getValue(); j++) {
                ids[j] = j + startID;
            }
            AvaStat avaStat = new AvaStat();

            for (int i = 0; i < entry.getKey().size(); i++) {
                int c = entry2.getValue() * entry2.getKey().get(i);
                if (c > 0) {
                    avaCounts.get(entry.getKey().get(i)).put(entry2.getKey(), c);
                }
            }
            startID += entry2.getValue();

            avaStat.ids = ids;
            avaStat.start = startid;
            avadeg.put(entry2.getKey(), avaStat);

        }
        avaInfo.put(entry.getKey().get(0).sourceTable, avadeg);
    }

}
