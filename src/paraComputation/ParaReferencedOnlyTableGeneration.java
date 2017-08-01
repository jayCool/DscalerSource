package paraComputation;

import db.structs.ComKey;
import db.structs.DB;
import dataStructure.AvaliableStatistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class ParaReferencedOnlyTableGeneration implements Runnable {

    public Map.Entry<String, HashMap<ArrayList<Integer>, AvaliableStatistics>> jdAvaEntry;
    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<Integer>>> origianlReverseJointDegrees;
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    public String outPath;
    public DB originalDB;
    public String delimiter;

 

    @Override
    public void run() {

        ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> concurrentJDAvaHashMap = new ConcurrentHashMap<>();
        copyToConcurrent(concurrentJDAvaHashMap);
        outputTable(concurrentJDAvaHashMap);

    }

    /**
     * Generate the concurrentHashMap
     *
     * @param concurrentJDAvaHashMap
     */
    private void copyToConcurrent(ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> concurrentJDAvaHashMap) {
        for (Map.Entry<ArrayList<Integer>, AvaliableStatistics> jdEntry : jdAvaEntry.getValue().entrySet()) {
            ArrayList<Integer> idArray = new ArrayList<>(jdEntry.getValue().ids.length);
            for (int id : jdEntry.getValue().ids) {
                idArray.add(id);
            }
            concurrentJDAvaHashMap.put(jdEntry.getKey(), idArray);
        }
    }

    /**
     * Output the table
     * @param concurrentJDAvaHashMap 
     */
    private void outputTable(ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> concurrentJDAvaHashMap) {

        BufferedWriter pw = null;
        try {
            File file = new File(outPath + "/" + jdAvaEntry.getKey() + ".txt");
            pw = new BufferedWriter(new FileWriter(file), 100000);

            int level = 0;
            int tableNum = this.originalDB.getTableID(jdAvaEntry.getKey());

            while (concurrentJDAvaHashMap.keySet().size() > 0) {
                for (Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry : concurrentJDAvaHashMap.entrySet()) {
                    int frequency = jdEntry.getValue().size();
                    ArrayList<Integer> closestJD = jdEntry.getKey();
                    if (level != 0) {
                        closestJD = calculateClosestJD(jdEntry);
                        if (closestJD.size() == 0) {
                            continue;
                        }
                    }
                    outputTuples(jdEntry, closestJD, concurrentJDAvaHashMap, frequency, pw, tableNum);
                }
                level++;
            }
            pw.close();
        } catch (IOException ex) {
            Logger.getLogger(ParaReferencedOnlyTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                pw.close();
            } catch (IOException ex) {
                Logger.getLogger(ParaReferencedOnlyTableGeneration.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    
    /**
     * 
     * @param jdEntry
     * @return closestJD
     */
    private ArrayList<Integer> calculateClosestJD(Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry) {
        ArrayList<ArrayList<Integer>> closestJointDegrees = calculateClosestJDs(jdEntry.getKey(), origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).keySet());
        Random rand = new Random();
        int retrivalIndex = rand.nextInt(closestJointDegrees.size());
        ArrayList<Integer> calculatedJD = closestJointDegrees.get(retrivalIndex);
        closestJointDegrees.remove(retrivalIndex);
        while (origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(calculatedJD).isEmpty()) {
            origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).remove(calculatedJD);
            if (closestJointDegrees.size() == 0) {
                return new ArrayList<>();
            }
             retrivalIndex = rand.nextInt(closestJointDegrees.size());
            calculatedJD = closestJointDegrees.get(retrivalIndex);
            closestJointDegrees.remove(retrivalIndex);
        }
        return calculatedJD;
    }
    
       
    
    /**
     * Calculates the closest JDs
     * @param jointDegree
     * @param originalJDSet
     * @return closestJDs
     */
    private ArrayList<ArrayList<Integer>> calculateClosestJDs(ArrayList<Integer> jointDegree, Set<ArrayList<Integer>> originalJDSet) {
        int maxError = 0;

        HashMap<Integer, ArrayList<ArrayList<Integer>>> vectorDiffMap = new HashMap<>();

        for (ArrayList<Integer> originalJD : originalJDSet) {
            int diff = 0;
            for (int i = 0; i < jointDegree.size(); i++) {
                diff += Math.abs(jointDegree.get(i) - originalJD.get(i));
            }
            maxError = Math.max(maxError, Math.abs(diff));

            if (!vectorDiffMap.containsKey(Math.abs(diff))) {
                vectorDiffMap.put(Math.abs(diff), new ArrayList<ArrayList<Integer>>());
            }
            vectorDiffMap.get(Math.abs(diff)).add(originalJD);
        }

        ArrayList<ArrayList<Integer>> closestJDs = new ArrayList<>();
        for (int i = 0; i <= maxError; i++) {
            if (vectorDiffMap.containsKey(i)) {
                closestJDs.addAll(vectorDiffMap.get(i));
                break;
            }
        }
        return closestJDs;

    }
    
    
    /**
     * Output the tuples
     * @param jdEntry
     * @param closestJD
     * @param concurrentJDAvaHashMap
     * @param frequency
     * @param pw
     * @param tableNum
     * @throws IOException 
     */
    private void outputTuples(Map.Entry<ArrayList<Integer>, ArrayList<Integer>> jdEntry, ArrayList<Integer> closestJD,
            ConcurrentHashMap<ArrayList<Integer>, ArrayList<Integer>> concurrentJDAvaHashMap, 
            int frequency, BufferedWriter pw, int tableNum) throws IOException {
        if (origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).containsKey(closestJD) && origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(closestJD).size() > 0) {
            int numberOfOriginalIDs = origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(closestJD).size();
            
            ArrayList<Integer> originalIDs = new ArrayList<>();
            for (int i : origianlReverseJointDegrees.get(this.mergedDegreeTitle.get(jdAvaEntry.getKey())).get(closestJD)) {
                originalIDs.add(i);
            }
            Collections.shuffle(originalIDs);
            
            for (int i = 0; i < frequency; i++) {
                pw.write("" + jdEntry.getValue().get(i));
                int pkidNum = originalIDs.get(i % numberOfOriginalIDs);
                if (this.originalDB.tables[tableNum].nonKeys[pkidNum] != null) {
                    pw.write(delimiter + this.originalDB.tables[tableNum].nonKeys[pkidNum].trim());
                }
                pw.newLine();

            }
            concurrentJDAvaHashMap.remove(jdEntry.getKey());
        }
    }

}
