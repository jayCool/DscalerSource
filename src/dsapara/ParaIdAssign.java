/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import dbstrcture.DB;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author workshop
 */
public class ParaIdAssign implements Runnable {

    //  HashMap<String, HashMap<Integer, ArrayList<Integer>>> assignIDs;
    // HashMap<String, int[][]> efficientAssignIDs;
    Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution;
    HashMap<String, ArrayList<ComKey>> avaMaps;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs;
    HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    HashMap<String, Integer> scaleTableSize;
    HashMap<String, Integer> tableSize;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> reverseDistribution;
    String outFile;
    double s;
    //HashMap<String, HashMap<String, ArrayList<String>>> tuples;
    DB originalDB;
    HashMap<String, ArrayList<ComKey>> referencingTable;

    ParaIdAssign(
            Map.Entry<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> entry,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledCorrelationDistribution,
            HashMap<String, ArrayList<ComKey>> avaMaps,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, ArrayList<Integer>>> referencingIDs,
            HashMap<String, HashMap<ArrayList<Integer>, AvaStat>> avaInfo) {
        //   this.assignIDs = assignIDs;
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
    BufferedWriter pw;
    int tuplesize = 0;
    int tableNum = 0;
    //  boolean[][] scaleSize;
    long[] pk1 = new long[2];

    void newAssigning() throws IOException {
        File file = new File(outFile + "/" + entry.getKey() + ".txt");
        FileWriter writer = new FileWriter(file);
        pw = new BufferedWriter(writer, 100000);
        //   tuplesize = tuples.get(entry.getKey()).values().iterator().next().size();
        tableNum = this.originalDB.getTableNum(entry.getKey());
        tuplesize = this.originalDB.tables[tableNum].tuplesize;
        //  tuplesize = this.originalDB.tables[tableNum].fks[0].length + 1;
        referTable = entry.getKey();
        indexes = new int[avaMaps.get(entry.getKey()).size()];
        for (int i = 0; i < indexes.length; i++) {
            ComKey ck = avaMaps.get(entry.getKey()).get(i);
            indexes[i] = this.mergedDegreeTitle.get(ck.sourceTable).indexOf(ck);
        }
        System.err.println(entry.getKey() + "Start");
        id = 0;
        boolean flag = false;
        sortDis(reverseDistribution.get(entry.getKey()).keySet());
        int printed = 0;
        int[] size1 = new int[2];
        for (int i = 0; i < referencingTable.get(referTable).size(); i++) {
            String s = referencingTable.get(referTable).get(i).sourceTable;
            pk1[i] = this.scaleTableSize.get(s);
        }
        if (referencingTable.get(referTable).size() == 2) {
            //     scaleSize = new boolean[size1[0]][size1[1]];
        }
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry2 : entry.getValue().entrySet()) {

            if (entry2.getKey().size() == 1) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                }
                oneKeyGen(entry2);

            } else if (entry2.getKey().size() == 2 && entry2.getValue() > 0) {
                if (!flag) {
                    headattr0 = avaMaps.get(entry.getKey()).get(0);
                    headattr1 = avaMaps.get(entry.getKey()).get(1);
                    index0 = this.mergedDegreeTitle.get(headattr0.sourceTable).indexOf(headattr0);
                    index1 = this.mergedDegreeTitle.get(headattr1.sourceTable).indexOf(headattr1);
                    flag = true;
                }
                printed += entry2.getValue();
                twoKeyGen(entry2);
            } else {
                threeKeyGen(entry2);

            }
        }
        if (referencingTable.get(referTable).size() == 2) {
            ArrayList<Integer> arr1 = new ArrayList<>();
            ArrayList<Integer> arr2 = new ArrayList<>();
            for (int i = 0; i < pk1[0]; i++) {
                arr1.add(i);
            }
            for (int i = 0; i < pk1[1]; i++) {
                arr2.add(i);
            }

            while (printed < this.scaleTableSize.get(this.referTable)) {
                System.out.println("Start to print Added: " + (printed - this.scaleTableSize.get(this.referTable)) +"  "+this.referTable);
                System.out.println("pk11111   " + pk1[1] +"           pk[0]: " + pk1[0]);
                boolean found = false;
                Collections.shuffle(arr1);
                for (long i = 0; i < arr1.size() && printed < this.scaleTableSize.get(this.referTable); i++) {
                    int k = 0;
                    while (k <= 100) {
                        long j = (long)( pk1[1]*Math.random());
                        long newid =  pk1[0]*i + j;
                      //  System.out.println(newid);
                        if (!this.usedId.contains(newid)) {
                            pw.write("" + id);
                            pw.write(deli + i + deli + j);
                            int matchId = (int) Math.floor(this.originalDB.tables[tableNum].nonKeys.length * Math.random());
                            if (this.originalDB.tables[this.tableNum].nonKeys[matchId] != null) {
                                pw.write(deli + this.originalDB.tables[this.tableNum].nonKeys[matchId]);
                            }
                            pw.newLine();
                            id++;

                            usedId.add(newid);
                            printed++;
                            break;
                        }
                        k++;
                    }
                }

                //pw.
            }
        }
        pw.close();

        reverseDistribution.remove(this.referTable);
        scaledCorrelationDistribution.remove(this.referTable);

        System.err.println(entry.getKey() + " printed: " + printed);
    }

    @Override
    public void run() {
        try {
            newAssigning();
        } catch (IOException ex) {
            Logger.getLogger(ParaIdAssign.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    ArrayList<Integer> tempR = new ArrayList<>();

    private void sortDis(Set<ArrayList<ArrayList<Integer>>> keySet) {

        for (ArrayList<ArrayList<Integer>> arr : keySet) {
            int sum = 0;
            for (ArrayList<Integer> karr : arr) {
                for (int i : karr) {
                    sum += i;
                }
            }
            if (!mapping.containsKey(sum)) {
                mapping.put(sum, new ArrayList<ArrayList<ArrayList<Integer>>>());
            }
            mapping.get(sum).add(arr);
        }

    }
    HashMap<Integer, ArrayList<ArrayList<ArrayList<Integer>>>> mapping = new HashMap<>();

    private void oneKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2) throws IOException {
        int k = 0;
        ArrayList<Integer> degrees = entry2.getKey().get(k);
        int count = entry2.getValue();

        int[] queues = this.avaInfo.get(headattr0.sourceTable).get(degrees).ids;
        ArrayList<ArrayList<Integer>> nearDegree = nearDegree(entry2.getKey());

        for (int i = 0; i < count / queues.length * queues.length; i++) {
            //  String matchId = ;
            printFile(queues[i % queues.length], id, idAssign(nearDegree));
            id++;
        }

        int c = this.avaInfo.get(headattr0.sourceTable).get(degrees).start[index0];

        for (int i = count / queues.length * queues.length; i < count; i++) {
            //  String matchId = ;
            printFile(queues[c], id, idAssign(nearDegree));
            c++;
            c = c % queues.length;
            id++;
        }
        nearDegree = null;
        this.avaInfo.get(headattr0.sourceTable).get(degrees).start[index0] = c;

    }

    ComKey headattr0;
    int index0, index1;
    ComKey headattr1;
//    ComKey headattr1;

    private void twoKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2) throws IOException {
        int count = entry2.getValue();
        int q1Len = this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids.length;
        int c = this.avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).start[index0];

        int[] firstCols = new int[q1Len];
    //    System.out.println(q1Len + "  " + this.entry.getKey());
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

        int q2Len = this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).ids.length;
        int c2 = this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).start[index1];
        ArrayList<ArrayList<Integer>> nearDegree = nearDegree(entry2.getKey());

        for (int j = 0; j < firstCols.length; j++) {
            for (int i = 0; i < firstCols[j]; i++) {
                printFile(avaInfo.get(headattr0.sourceTable).get(entry2.getKey().get(0)).ids[j],
                        avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).ids[c2], id, idAssign(nearDegree));

                c2++;
                c2 = c2 % q2Len;
                id++;
            }
        }
        nearDegree = null;
        firstCols = null;
        this.avaInfo.get(headattr1.sourceTable).get(entry2.getKey().get(1)).start[index1] = c2;
    }

    private void threeKeyGen(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2
    ) throws IOException {
        Queue<ArrayList<Integer>> queue = new LinkedList<>();

        for (int i = 0; i < entry2.getValue(); i++) {
            queue.offer(new ArrayList<Integer>());
        }

        int count = entry2.getValue();

        for (int k = 0; k < entry2.getKey().size() && entry2.getValue() != 0; k++) {
            ArrayList<Integer> headdegrees = entry2.getKey().get(k);
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
        ArrayList<ArrayList<Integer>> nearDegree = nearDegree(entry2.getKey());

        while (!queue.isEmpty()) {
            Object[] t = queue.poll().toArray();
            int c = 0;
            int[] tc = new int[t.length];
            for (Object k : t) {
                tc[c] = (int) k;

            }
            int matchId = idAssign(nearDegree);
            printFile(tc, id, matchId);
            id++;
        }

        nearDegree = null;
    }
    Random rand = new Random();
    int nearsize = 0;

    private int idAssign(ArrayList<ArrayList<Integer>> calDeg) {

        int temp = rand.nextInt(nearsize);

        return reverseDistribution.get(entry.getKey()).get(calDeg).get(temp);

    }

    private ArrayList<ArrayList<Integer>> nearestSum(ArrayList<ArrayList<Integer>> key) {
        int sum1 = getSum(key);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (mapping.containsKey(sum1 + i)) {
                int size = mapping.get(sum1 + i).size();
                if (size == 0) {
                    mapping.remove(sum1 + i);
                } else {
                    int can = (int) (Math.random() * (size - 1) + 0.45);
                    return mapping.get(sum1 + i).get(can);
                }
            }
            if (mapping.containsKey(sum1 - i)) {
                int size = mapping.get(sum1 - i).size();
                int can = (int) (Math.random() * (size - 1) + 0.45);
                return mapping.get(sum1 - i).get(can);
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

    private void idAssign(Entry<ArrayList<ArrayList<Integer>>, Integer> entry2) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private ArrayList<ArrayList<Integer>> nearDegree(ArrayList<ArrayList<Integer>> key) {
        if (reverseDistribution.get(entry.getKey()).containsKey(key)) {
            ArrayList<ArrayList<Integer>> calDeg = key;
            nearsize = reverseDistribution.get(entry.getKey()).get(calDeg).size();
            return calDeg;
        }
        ArrayList<ArrayList<Integer>> calDeg = nearestSum(key);
        while (reverseDistribution.get(entry.getKey()).get(calDeg).size() == 0) {
            reverseDistribution.get(entry.getKey()).remove(calDeg);
            int sum1 = 0;
            sum1 = getSum(calDeg);
            mapping.get(sum1).remove(calDeg);
            if (mapping.get(sum1).size() == 0) {
                mapping.remove(sum1);
            }
            calDeg = nearestSum(key);
        }

        nearsize = reverseDistribution.get(entry.getKey()).get(calDeg).size();
        return calDeg;
    }
    String deli = "|";
    private void printFile(int[] t, int id, int matchId) throws IOException {
        int keySize = t.length;
        pw.write("" + id);
        for (int i : t) {
            pw.write(deli + i);
        }
        if (this.originalDB.tables[this.tableNum].nonKeys[matchId] != null) {
            pw.write(deli + this.originalDB.tables[this.tableNum].nonKeys[matchId]);
        }

        pw.newLine();
    }

    private void printFile(int t, int id, int matchId) throws IOException {
        int keySize = 1;
        pw.write("" + id);
        pw.write(deli + t);
        if (this.originalDB.tables[this.tableNum].nonKeys[matchId] != null) {
            pw.write(deli + this.originalDB.tables[this.tableNum].nonKeys[matchId]);
        }

        pw.newLine();
    }
    int printedTotal = 0;
    HashSet<Long> usedId = new HashSet<>();

    private void printFile(long t0, long t1, int id, int matchId) throws IOException {
        int keySize = 2;
        pw.write("" + id);
        pw.write(deli + t0 + deli + t1);
        // this.scaleSize[t0][t1] = true;
        usedId.add(t0 * pk1[0] + t1);
        if (this.originalDB.tables[this.tableNum].nonKeys[matchId] != null) {
            pw.write(deli + this.originalDB.tables[this.tableNum].nonKeys[matchId]);
        }

        pw.newLine();
    }

    private Long Long(int i) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
