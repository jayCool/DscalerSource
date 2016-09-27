/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

/**
 *
 * @author workshop
 */
public class ParaIdAssign_1 implements Runnable {

    HashMap<String, HashMap<Integer, ArrayList<Integer>>> assignIDs;
    HashMap<String, int[][]> efficientAssignIDs;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution;
    HashMap<String, ArrayList<ComKey>> avaMaps;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs;
    HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    HashMap<String, Integer> scaleTableSize;
    HashMap<String, Integer> tableSize;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<String>>> reverseDistribution;

    ParaIdAssign_1(HashMap<String, HashMap<Integer, ArrayList<Integer>>> assignIDs,
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, ArrayList<ComKey>> avaMaps,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo) {
        this.assignIDs = assignIDs;
        this.entry = entry;
        this.scaledCorrelationDistribution = scaledCorrelationDistribution;

        this.avaMaps = avaMaps;
        this.referencingIDs = referencingIDs;
        this.avaInfo = avaInfo;
    }

    String referTable = "";
    int id = 1;
    int[] indexes;
    boolean noKey = true;
    void newAssigning() {
        referTable = entry.getKey();
        indexes = new int[avaMaps.get(entry.getKey()).size()];
        for (int i = 0; i < indexes.length; i++) {
            ComKey ck = avaMaps.get(entry.getKey()).get(i);
            indexes[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }
       // HashMap<Integer, ArrayList<Integer>> ids = new HashMap<>();
       int[][] ids=new int[this.scaleTableSize.get(referTable)+1][this.tableSize.get(referTable)];  //starting id is 1. hence 0 is avoided.
        System.err.println(entry.getKey() + "Start");
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap = new HashMap<>();
        id = 1;
        boolean flag = false;
       
        if (!avaInfo.containsKey(entry.getKey())){
            noKey = true;
        }
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : entry.getValue().entrySet()) {

            if (!mmap.containsKey(entry2.getKey())) {
                mmap.put(entry2.getKey(), new ArrayList<Integer>());
            }

            if (entry2.getKey().size() == 1) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                }
                oneKeyGen(entry2, mmap, ids);

            } else if (entry2.getKey().size() == 2 && entry2.getValue() > 0) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    headattr1 = avaMaps.get(entry.getKey()).get(1);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                    index1 = this.mergedDegreeTitle.get(headattr1.sourceTable).indexOf(headattr1);
                    flag = true;
                }
                twoKeyGen(entry2, mmap, ids);
            } else {
                threeKeyGen(entry2, mmap, ids);

            }
        }
     //   this.efficientAssignIDs.put(entry.getKey(), ids);
        referencingIDs.put(entry.getKey(), mmap);
        System.err.println(entry.getKey() + "End");
    }

    /*
     void noEffinewAssigning() {
        referTable = entry.getKey();
        indexes = new int[avaMaps.get(entry.getKey()).size()];
        for (int i = 0; i < indexes.length; i++) {
            ComKey ck = avaMaps.get(entry.getKey()).get(i);
            indexes[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }
        HashMap<Integer, ArrayList<Integer>> ids = new HashMap<>();
        System.err.println(entry.getKey() + "Start");
        HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap = new HashMap<>();
        id = 1;
        boolean flag = false;
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : entry.getValue().entrySet()) {

            if (!mmap.containsKey(entry2.getKey())) {
                mmap.put(entry2.getKey(), new ArrayList<Integer>());
            }

            if (entry2.getKey().size() == 1) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                }
                oneKeyGen(entry2, mmap, ids);

            } else if (entry2.getKey().size() == 2 && entry2.getValue() > 0) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    headattr1 = avaMaps.get(entry.getKey()).get(1);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                    index1 = this.mergedDegreeTitle.get(headattr1.sourceTable).indexOf(headattr1);
                    flag = true;
                }
                twoKeyGen(entry2, mmap, ids);
            } else {
                threeKeyGen(entry2, mmap, ids);

            }
        }
        assignIDs.put(entry.getKey(), ids);
        referencingIDs.put(entry.getKey(), mmap);
        System.err.println(entry.getKey() + "End");
    }
*/
    @Override
    public void run() {
        newAssigning();
    }

    ArrayList<Integer> tempR = new ArrayList<>();

    private void oneKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap, int[][] ids) {
        int k = 0;
        ArrayList<Integer> degrees = entry2.getKey().get(k);
        ComKey attr = avaMaps.get(entry.getKey()).get(k);
        int count = entry2.getValue();
        // queue.clear();
        int[] queues = this.avaInfo.get(headattr0.sourceTable).get(degrees).ids;
        
        for (int i = 0; i < count / queues.length * queues.length; i++) {
            
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((Integer) queues[i % queues.length]);
           // ids.put(id, tmp);
            int [] t = new int[1];
            t[0] =  queues[i % queues.length];
            ids[id] = t;
            mmap.get(entry2.getKey()).add(id);
            id++;
        }

        int c = this.avaInfo.get(headattr0.sourceTable).get(degrees).start[index0];

        for (int i = count / queues.length * queues.length; i < count; i++) {
          ArrayList<Integer> tmp = new ArrayList<>();
         //   int tmpid =;
            int[] t = new int[1];
            t[0] = queues[c];
          //  tmp.add(tmpid);
            ids[id] = t;
         //   ids.put(id, tmp);
            c++;
            c = c % queues.length;
            mmap.get(entry2.getKey()).add(id);
            id++;
        }
        this.avaInfo.get(headattr0.sourceTable).get(degrees).start[index0] = c;

    }

    ComKey headattr0;
    int index0, index1;
    ComKey headattr1;

    //The memory is not optimized
    private void twoKeyGenNonEffi(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap, HashMap<Integer, ArrayList<Integer>> ids) {
        int count = entry2.getValue();
        int k = 0;

        ArrayList<Integer> headdegrees = entry2.getKey().get(k);
        int[] queues = this.avaInfo.get(headattr0.sourceTable).get(headdegrees).ids;
        int c = this.avaInfo.get(headattr0.sourceTable).get(headdegrees).start[index0];

        int[] firstCols = new int[queues.length];
        if (count >= queues.length) {
            for (int i = 0; i < queues.length; i++) {
                firstCols[i] = count / queues.length;
            }
        }

        for (int i = 0; i < count - count / queues.length * queues.length; i++) {
            firstCols[c] += 1;
            c++;
            c = c % queues.length;
        }

        this.avaInfo.get(headattr0.sourceTable).get(headdegrees).start[index0] = c;

        k = 1;
        headdegrees = entry2.getKey().get(k);

        int[] queues2 = this.avaInfo.get(headattr1.sourceTable).get(headdegrees).ids;
        int c2 = this.avaInfo.get(headattr1.sourceTable).get(headdegrees).start[index1];

        for (int j = 0; j < firstCols.length; j++) {
            for (int i = 0; i < firstCols[j]; i++) {
                ArrayList<Integer> temp = new ArrayList<>();
                temp.add((Integer) queues[j]);
                int tmpid = queues2[c2];
                c2++;
                c2 = c2 % queues2.length;
                temp.add(tmpid);
                ids.put(id, temp);
                mmap.get(entry2.getKey()).add(id);
                id++;
            }
        }

        this.avaInfo.get(headattr1.sourceTable).get(headdegrees).start[index1] = c2;
    }

    private void twoKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap, int[][] ids) {
        int count = entry2.getValue();
   //     int k = 0;

    //    ArrayList<Integer> headdegrees = entry2.getKey().get(0);
        //  int[] queues = 
        //        this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids;
        int q1Len = this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids.length;
        int c = this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).start[index0];

        int[] firstCols = new int[q1Len];
        if (count >= q1Len) {
            for (int i = 0; i < q1Len; i++) {
                firstCols[i] = count / q1Len;
            }
        }

        for (int i = 0; i < count - count / q1Len * q1Len; i++) {
            firstCols[c] += 1;
            c++;
            c = c % q1Len;
        }

        this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).start[index0] = c;

   //     k = 1;
        //   headdegrees = entry2.getKey().get(k);
        //  int[] queues2 = 
        //        this.avaInfo.get(headattr1.sourceTable).get( entry2.getKey().get(1)).ids;
        int q2Len = this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).ids.length;
        int c2 = this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).start[index1];

        for (int j = 0; j < firstCols.length; j++) {
            for (int i = 0; i < firstCols[j]; i++) {
             //   ArrayList<Integer> temp = new ArrayList<>();
              //  temp.add(avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids[j]);  //first queue
               // temp.add(avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).ids[c2]);
               int[] t = new int[2];
               t[0] = avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids[j];
               t[1] = avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).ids[c2];
               ids[id] = t;
                c2++;
                c2 = c2 % q2Len;
               // ids.put(id, temp);
                mmap.get(entry2.getKey()).add(id);
                id++;
            }
        }
        firstCols=null;
        this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).start[index1] = c2;
    }

    private void threeKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>> mmap, int[][] ids) {
        Queue<ArrayList<Integer>> queue = new LinkedList<>();

        for (int i = 0; i < entry2.getValue(); i++) {
            queue.offer(new ArrayList<Integer>());
        }

        int count = entry2.getValue();

        for (int k = 0; k < entry2.getKey().size() && entry2.getValue() != 0; k++) {
            ArrayList<Integer> headdegrees = entry2.getKey().get(k);
            //  ComKey headattr = avaMaps.get(entry.getKey()).get(k);
            int[] idds = this.avaInfo.get(avaMaps.get(entry.getKey()).get(k).sourceTable).get(headdegrees).ids;
            int c = this.avaInfo.get(avaMaps.get(entry.getKey()).get(k).sourceTable).get(headdegrees).start[indexes[k]];

            for (int i = 0; i < count; i++) {

                int ele = idds[c];
                c++;
                c = c % idds.length;

                ArrayList<Integer> arr = new ArrayList<>();
                ArrayList<Integer> pre = queue.poll();
                for (int j = 0; j < pre.size(); j++) {
                    arr.add(pre.get(j));
                }
                arr.add(ele);
                queue.offer(arr);
            }
            this.avaInfo.get(avaMaps.get(entry.getKey()).get(k).sourceTable).get(headdegrees).start[indexes[k]] = c;
            HashMap<ArrayList<Integer>, Integer> maps = new HashMap<>();
            while (!queue.isEmpty()) {
                if (!maps.containsKey(queue.peek())) {
                    maps.put(queue.peek(), 0);
                }
                maps.put(queue.peek(), maps.get(queue.poll()) + 1);
            }

            for (Entry<ArrayList<Integer>, Integer> entry : maps.entrySet()) {
                for (int t = 0; t < entry.getValue(); t++) {
                    queue.offer(entry.getKey());
                }
            }
        }

        while (!queue.isEmpty()) {
            mmap.get(entry2.getKey()).add(id);
          //  ids.put(id, queue.poll());
            Object[] t = queue.poll().toArray();
            int c = 0;
            int[] tc = new int[t.length];
            for (Object k:t){
                tc[c] = (int) k;
            
            }
            ids[id] = tc;
         //   ids[id] = ;
            id++;
        }
    }

}
