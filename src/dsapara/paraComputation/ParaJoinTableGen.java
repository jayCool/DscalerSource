package dsapara.paraComputation;

import dscaler.dataStruct.AvaliableStatistics;
import dbstrcture.DB;
import dbstrcture.ComKey;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author workshop
 */
public class ParaJoinTableGen implements Runnable {

    public Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jdAvaEntry;
    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> reverseMergedDegree;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public String outPath;
    public DB originalDB;

    private ArrayList<ArrayList<Integer>> closestDegreeSets(ArrayList<Integer> jd, Set<ArrayList<Integer>> jdSets) {
        int maxAbsError = 0;

        HashMap<Integer, ArrayList<ArrayList<Integer>>> vectorDiffMap = new HashMap<>();

        for (ArrayList<Integer> originalJD : jdSets) {
            int diff = 0;
            for (int i = 0; i < jd.size(); i++) {
                diff += Math.abs(jd.get(i) - originalJD.get(i));
            }
            maxAbsError = Math.max(maxAbsError, Math.abs(diff));

            if (!vectorDiffMap.containsKey(Math.abs(diff))) {
                vectorDiffMap.put(Math.abs(diff), new ArrayList<ArrayList<Integer>>());
            }
            vectorDiffMap.get(Math.abs(diff)).add(originalJD);
        }

        ArrayList<ArrayList<Integer>> closestMap = new ArrayList<>();
        for (int i = 0; i <= maxAbsError; i++) {
            if (vectorDiffMap.containsKey(i)) {
                closestMap.addAll(vectorDiffMap.get(i));
                break;
            }
        }
        return closestMap;

    }

    @Override
    public void run() {

        BufferedWriter pw = null;
        try {
            File file = new File(outPath + "/" + jdAvaEntry.getKey() + ".txt");
            pw = new BufferedWriter(new FileWriter(file), 100000);
            ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> conJDAvaEntry = new ConcurrentHashMap<>();
            copyToConcurrent(conJDAvaEntry);
            printTuples(conJDAvaEntry, pw);
        } catch (IOException ex) {
            Logger.getLogger(ParaJoinTableGen.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                pw.close();
            } catch (IOException ex) {
                Logger.getLogger(ParaJoinTableGen.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private void copyToConcurrent(ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> conJDAvaEntry) {
        for (Map.Entry<ArrayList<Integer>, AvaliableStatistics> jdEntry : jdAvaEntry.getValue().entrySet()) {
            ArrayList<Integer> idArr = new ArrayList<>(jdEntry.getValue().ids.length);
            for (int t : jdEntry.getValue().ids) {
                idArr.add(t);
            }
            conJDAvaEntry.put(jdEntry.getKey(), idArr);
        }
    }

    private void printTuples(ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> conJDAvaEntry, BufferedWriter pw) throws IOException {
        int level = 0;
        int tableNum = this.originalDB.getTableNum(jdAvaEntry.getKey());

        while (conJDAvaEntry.keySet().size() > 0) {
            for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry : conJDAvaEntry.entrySet()) {
                int leftOver = jdEntry.getValue().size();
                ArrayList<Integer> calDeg = jdEntry.getKey();
                if (level != 0) {
                    calDeg = computeCandidateDegree(jdEntry);
                    if (calDeg.size() == 0) {
                        continue;
                    }
                }
                updateStats(jdEntry, calDeg, conJDAvaEntry, leftOver, pw, tableNum);
            }
            level++;
        }
        pw.close();
    }

  

    private ArrayList<Integer> computeCandidateDegree(Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry) {
        ArrayList<ArrayList<Integer>> calDegs = closestDegreeSets(jdEntry.getKey(), reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).keySet());
        ArrayList<Integer> calDeg = calDegs.get((int) (Math.random() * (calDegs.size() - 1) + 0.49));
        while (reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(calDeg).isEmpty()) {
            reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).remove(calDeg);
            if (calDegs.size() == 0) {
                return new ArrayList<>();
            }

            calDeg = calDegs.get((int) (Math.random() * (calDegs.size() - 1) + 0.49));
            calDegs.remove(calDeg);
        }
        return calDeg;
    }

    private void updateStats(Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry, ArrayList<Integer> calDeg,
            ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> conJDAvaEntry, int leftOver, BufferedWriter pw, int tableNum) throws IOException {
        if (reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).containsKey(calDeg) && reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(calDeg).size() > 0) {
            int size = reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(calDeg).size();
            ArrayList<Integer> oldIDs = new ArrayList<>();
            for (int i : reverseMergedDegree.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(calDeg)) {
                oldIDs.add(i);
            }
            Collections.shuffle(oldIDs);
            for (int i = 0; i < leftOver; i++) {
                pw.write("" + jdEntry.getValue().get(i));
                int pkidNum = oldIDs.get(i % size);
                if (this.originalDB.tables[tableNum].nonKeys[pkidNum] != null) {
                    pw.write("|" + this.originalDB.tables[tableNum].nonKeys[pkidNum]);
                }
                pw.newLine();

            }
            conJDAvaEntry.remove(jdEntry.getKey());
        }
    }

}
