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
public class StaticUpGlobalRefTableGen implements Runnable {

   
    int leftOver = 0;
    int start = 0;
    
     double s;
    int startInt;
    int endInt;
    HashMap<Integer, String> mmap = new HashMap<>();
    int[] countH;
    int[] countL;
    int[] referenceSize;
    int[] reverseSize;
    ArrayList<Integer> indexes;
    ArrayList<ArrayList<String>> reverseids;
   ArrayList<ArrayList<Integer>> referenceids;
    
    boolean staticUp = true;

    @Override
    public void run() {

        for (int pos = startInt; pos < endInt; pos++) {
            start = 0;
            int entrySize = reverseSize[pos];
            leftOver = referenceSize[pos] - start;
            if (leftOver == 0) {
                continue;
            }
                produceValueShort(countL, countH, mmap, indexes, pos, entrySize, reverseids.get(pos), referenceids.get(pos));
         
        }
        System.out.println("Out");
    }

    private void produceValueShort(int[] countL, int[] countH, HashMap<Integer, String> mmap, ArrayList<Integer> indexes, int pos,
             int entrySize, ArrayList<String> reverseids, ArrayList<Integer> referenceids) {
        int cap = (int) Math.ceil(entrySize * this.s);
        int oldV = countL[pos];
        int minV = Math.min(oldV, leftOver);
        int q = cap - oldV;

        for (int i = 0; i < minV; i++) {
            q = q % entrySize;
            String rrid = reverseids.get(q);
            mmap.put(referenceids.get(start), rrid);
            q++;
            start++;
        }
        
        
    }

}
