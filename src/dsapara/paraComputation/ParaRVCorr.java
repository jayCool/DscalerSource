package dsapara.paraComputation;

import db.structs.ComKey;
import dscaler.dataStruct.CoDa;

import dsapara.Sort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author workshop
 */
public class ParaRVCorr implements Runnable {

    public boolean eveNum = true;
    HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts;
    HashMap<ArrayList<ArrayList<Integer>>, Integer> originalRVDis;
    HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis;
    HashMap<String, ArrayList<ComKey>> mergedDegreeTitle;
    String ekey;
    HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap;
    HashSet<ArrayList<ArrayList<Integer>>> boundTrash = new HashSet<>();
    HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap = new HashMap<>();
    HashMap<ArrayList<ArrayList<Integer>>, Integer> allBounds = new HashMap<>();
    boolean terminated = false;
    public double sRatio;
    double sE = 0;

    public String curTable;
    public HashMap<String, ArrayList<ComKey>> referenceTable;
    int budget = 0;
    int iteration = 0;
    int indexcount = 0;
    long starterRandom = 0;
    int thresh = 5;
    boolean[] fk1;
    boolean[] fk2;

    public HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, ArrayList<ArrayList<Integer>>>> mappedBestJointDegree;
    public HashMap<String, Boolean> uniqueNess;
    public CoDa originalCoDa;
    ArrayList<List<Entry<ArrayList<Integer>, Integer>>> sortedJDs = new ArrayList<>();
    ArrayList<ArrayList<Boolean>> fks = new ArrayList<>();

    public ParaRVCorr(HashMap<ComKey, HashMap<Integer, ArrayList<ArrayList<Integer>>>> jdSumMap,
            HashMap<String, HashMap<ArrayList<ArrayList<Integer>>, Integer>> scaledRVDisMap,
            HashMap<ComKey, HashMap<ArrayList<Integer>, Integer>> ckJDAvaCounts,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> originalRVDis,
            HashMap<ArrayList<ComKey>, HashMap<ArrayList<Integer>, Integer>> scaledJDDis,
            HashMap<String, ArrayList<ComKey>> mergedDegreeTitle) {
        this.ckJDAvaCounts = ckJDAvaCounts;
        this.originalRVDis = originalRVDis;
        this.scaledJDDis = scaledJDDis;
        this.mergedDegreeTitle = mergedDegreeTitle;
        this.scaledRVDisMap = scaledRVDisMap;
        this.jdSumMap = jdSumMap;
    }

    @Override
    public void run() {
        scaledRVDisMap.put(this.curTable, rvCorrelation());
        terminated = true;

    }

    private HashMap<ArrayList<ArrayList<Integer>>, Integer> rvCorrelation() {
        HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis = new HashMap<>();

        ArrayList<ArrayList<ComKey>> comkeys = new ArrayList<>();
        sortJDBasedOnValue(comkeys);
        List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortedRV = sortRVBasedOnJDAppearance(comkeys);

        correlateRV(scaledRVDis, sortedRV);

        while (iteration < 4 && checkAvaCoutsNotEmpty()) {
            correlateRV(scaledRVDis, sortedRV);
        }

        iteration = 5;
        if (checkAvaCoutsNotEmpty()) {
            System.err.println("sortedRV.get(0).getKey(): " + sortedRV.get(0).getKey());
            System.err.println(ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(0)));
            System.err.println(this.curTable + ": is not empty!");
        }
        if (sortedRV.get(0).getKey().size() >= 3) {
            if (checkAvaCoutsNotEmpty()) {
                randomRoundEffiThreeKeys(scaledRVDis);
            }
        }
        if (checkAvaCoutsNotEmpty()) {
            System.out.println("here : sortedRV.get(0).getKey(): " + sortedRV.get(0).getKey());
            System.out.println(ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(0)));
            System.out.println(this.curTable + ": is not empty!");
        }

        if (sortedRV.get(0).getKey().size() == 2) {
            if (checkAvaCoutsNotEmpty()) {
                randomRoundEffiTwoKey(scaledRVDis);
            }
        }
        if (sortedRV.get(0).getKey().size() == 1) {
            if (checkAvaCoutsNotEmpty()) {
                randomRoundEffiOneKey(scaledRVDis);
            }
        }

        if (sortedRV.get(0).getKey().size() == 2) {
            if (checkAvaCoutsNotEmpty()) {
                randomSwapEffi(scaledRVDis);
            }
        }

        if (checkAvaCoutsNotEmpty()) {
            System.err.println("sortedRV.get(0).getKey(): " + sortedRV.get(0).getKey());
            System.err.println(ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(0)));
            System.err.println(this.curTable + ": is not empty!");
        }

        return scaledRVDis;
    }

    private boolean twoElement(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2,
            ArrayList<ArrayList<Integer>> rv, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis, int k, int arrSize) {
        if (arrSize != conScaledRVDis.getKey().size()) {
            return false;
        }
        for (int i = 0; i < k; i++) {
            pair1.add(rv.get(i));
            pair2.add(conScaledRVDis.getKey().get(i));
        }
        pair2.add(rv.get(k));
        pair1.add(conScaledRVDis.getKey().get(k));

        for (int i = k + 1; i < arrSize; i++) {
            pair1.add(rv.get(i));
            pair2.add(conScaledRVDis.getKey().get(i));
        }
        return true;
    }

    private int ivConstraint(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2,
            ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis,
            int bound1, int bound2, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry, int val) {

        int totalValue = conScaledRVDis.get(entry.getKey());
        if (!conScaledRVDis.containsKey(pair1)) {
            conScaledRVDis.put(pair1, 0);
        }
        if (!conScaledRVDis.containsKey(pair2)) {
            conScaledRVDis.put(pair2, 0);
        }
        int v1 = bound1 - conScaledRVDis.get(pair1);
        int v2 = bound2 - conScaledRVDis.get(pair2);
        int minValue = Math.min(v1, v2);
        minValue = Math.min(minValue, totalValue);
        minValue = Math.min(minValue, val);
        return minValue;
    }

    private int updateCon(ArrayList<ArrayList<Integer>> pair1, ArrayList<ArrayList<Integer>> pair2, int vsub, int val, int bound1, int bound2, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> con) {
        con.put(pair1, vsub + con.get(pair1));
        con.put(pair2, vsub + con.get(pair2));

        val = val - vsub;

        if (bound1 < con.get(pair1)) {
            System.err.println("pair1: " + pair1 + "    " + bound1 + "  " + con.get(pair1));
        }
        if (bound2 < con.get(pair2)) {
            System.err.println("pair2: " + pair2 + "    " + bound2 + "  " + con.get(pair2));
        }
        return val;
    }

    private void correlateRV(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis,
            List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortedRV) {
        iteration++;
        for (int i = 0; i < sortedRV.size() && checkAvaCoutsNotEmpty(); i++) {

            int frequency = this.originalCoDa.rvDis.get(this.curTable).get(sortedRV.get(i).getKey());

            if (frequency == 0) {
                continue;
            }

            frequency = calFreq(frequency);

            if (!eveNum || (this.eveNum && this.checkSocial(sortedRV.get(i).getKey()))) {
                if (iteration == 1) {
                    initialUpdateValue(sortedRV.get(i).getKey(), scaledRVDis, frequency);
                }
                if (iteration != 1) {
                    updateValue(sortedRV.get(i).getKey(), scaledRVDis, frequency);
                }
            }
        }

    }

    private void updateValue(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis, int frequency) {

        boolean mappedBestFlag = checkAppearBest(originalRV);
        if (!mappedBestFlag) {
            return;
        }
        ArrayList<ArrayList<ArrayList<Integer>>> rvSet = new ArrayList<>();
        int totalNum = compRVSet(originalRV, rvSet);
        if (totalNum == -1) {
            return;
        }

        budget = frequency;
        this.starterRandom = -1;
        while (starterRandom < totalNum - 1 && budget > 0 && totalNum > 0) {
            starterRandom++;

            ArrayList<ArrayList<Integer>> calDegrees = fetchSingle(totalNum, rvSet, originalRV);
            if (calDegrees.size() == 0) {
                continue;
            }
            if (this.starterRandom >= totalNum) {
                break;
            }

            boolean flag = checkDegreeValid(calDegrees);
            if (!flag) {
                continue;
            }

            processCalDegrees(calDegrees, frequency, originalRV, scaledRVDis);

        }

        return;
    }

    private boolean checkAvaCoutsNotEmpty() {
        for (ComKey ck : referenceTable.get(this.curTable)) {
            ArrayList<ArrayList<Integer>> zeroKeys = new ArrayList<>();
            for (ArrayList<Integer> key : ckJDAvaCounts.get(ck).keySet()) {
                if (ckJDAvaCounts.get(ck).get(key) == 0) {
                    zeroKeys.add(key);
                }
            }
            for (ArrayList<Integer> key : zeroKeys) {
                ckJDAvaCounts.get(ck).remove(key);
            }
        }
        return ckJDAvaCounts.containsKey(this.referenceTable.get(this.curTable).get(0)) && !ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(0)).isEmpty();
    }

    private int calFreq(Integer frequency) {
        int result = (int) (frequency * sRatio * (1 + Math.max(0, sE)));
        double difff = frequency * sRatio * (1 + Math.max(0, sE)) - result;
        double kl = Math.random();
        if (kl < difff) {
            result++;
        }
        return result;
    }

    private void initialUpdateValue(ArrayList<ArrayList<Integer>> originalRV,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis, int frequency) {

        if (!checkDegreeValid(originalRV)) {
            return;
        }
        processCalDegrees(originalRV, frequency, originalRV, scaledRVDis);
        return;
    }

    private void updateIndividualValue(ArrayList<ArrayList<Integer>> calDegrees,
            HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis, int frequency,
            ArrayList<ArrayList<Integer>> originalRV) {

        int oldV = 0;
        if (scaledRVDis.containsKey(calDegrees)) {
            oldV = scaledRVDis.get(calDegrees);
        }

        if (this.uniqueNess.get(curTable)) {
            int bound = calUperBound(calDegrees);

            if (oldV + frequency >= bound) {
                this.allBounds.remove(calDegrees);
                this.boundTrash.add(calDegrees);
                frequency = bound - oldV;
            }
        }

        if (eveNum && checkEQ(calDegrees.get(0), calDegrees.get(1))) {
            frequency = frequency / 2;
        }

        if (frequency <= 0) {
            return;
        }

        scaledRVDis.put(calDegrees, frequency + oldV);

        for (int k = 0; k < calDegrees.size(); k++) {
            int v = ckJDAvaCounts.get(referenceTable.get(curTable).get(k)).get(calDegrees.get(k));
            ckJDAvaCounts.get(referenceTable.get(curTable).get(k)).put(calDegrees.get(k), v - frequency);
        }
        if (this.eveNum && this.checkEQ(calDegrees.get(0), calDegrees.get(1))) {
            budget = budget - 2 * frequency;
        } else {
            budget = budget - frequency;
        }

        for (int k = 0; k < calDegrees.size(); k++) {
            if (checkAvaCounts(calDegrees.get(k), k)) {
                ckJDAvaCounts.get(referenceTable.get(curTable).get(k)).remove(calDegrees.get(k));

                String sourceTable = this.referenceTable.get(curTable).get(k).sourceTable;

                if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(originalRV.get(k))) {
                    if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(originalRV.get(k)).contains(calDegrees.get(k))) {
                        this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(originalRV.get(k)).remove(calDegrees.get(k));
                    }
                }
                cleanDistanceMapD(k, calDegrees.get(k));
            }
        }
    }

    private boolean checkSocial(ArrayList<ArrayList<Integer>> calDegrees) {
        return !eveNum || (eveNum && checkOrder(calDegrees));

    }

    private int calUperBound(ArrayList<ArrayList<Integer>> calDegrees) {
        int bound = 1;

        if (this.allBounds.containsKey(calDegrees)) {
            bound = this.allBounds.get(calDegrees);
        } else if (calDegrees.size() == 1) {
            bound = Integer.MAX_VALUE;
        } else {
            for (int t = 0; t < calDegrees.size(); t++) {
                String sourceTable = this.referenceTable.get(curTable).get(t).sourceTable;
                if (bound >= Integer.MAX_VALUE / scaledJDDis.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t))) {
                    bound = Integer.MAX_VALUE;
                } else {
                    bound = bound * scaledJDDis.get(this.mergedDegreeTitle.get(sourceTable)).get(calDegrees.get(t));
                }
            }
            allBounds.put(calDegrees, bound);
        }

        return bound;
    }

    private boolean checkAvaCounts(ArrayList<Integer> degree, int k) {
        return (ckJDAvaCounts.get(referenceTable.get(curTable).get(k)).containsKey(degree) && ckJDAvaCounts.get(referenceTable.get(curTable).get(k)).get(degree) == 0);
    }

    private int degreeSum(ArrayList<Integer> degree) {
        int insum = 0;
        for (int jj : degree) {
            insum += jj;
        }
        return insum;
    }

    private void cleanDistanceMapD(int k, ArrayList<Integer> calDegrees) {
        int insum = degreeSum(calDegrees);
        ComKey ck = referenceTable.get(curTable).get(k);
        if (jdSumMap.containsKey(ck) && jdSumMap.get(ck).containsKey(insum)) {
            this.jdSumMap.get(referenceTable.get(curTable).get(k)).get(insum).remove(calDegrees);
            if (this.jdSumMap.get(referenceTable.get(curTable).get(k)).get(insum).isEmpty()) {
                this.jdSumMap.get(referenceTable.get(curTable).get(k)).remove(insum);
            }
        }

    }

    boolean earlyStop = true;

    private boolean checkDegreeValid(ArrayList<ArrayList<Integer>> calDegrees) {
        for (int i = 0; i < calDegrees.size(); i++) {
            if (!ckJDAvaCounts.get(referenceTable.get(curTable).get(i)).containsKey(calDegrees.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void processCalDegrees(ArrayList<ArrayList<Integer>> calDegrees, int frequency,
            ArrayList<ArrayList<Integer>> originalRV, HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis) {
        for (int i = 0; i < calDegrees.size(); i++) {
            frequency = Math.min(frequency, ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(i)).get(calDegrees.get(i)));
        }

        if (!this.uniqueNess.get(curTable) || !this.boundTrash.contains(calDegrees) && checkSocial(calDegrees)) {
            updateIndividualValue(calDegrees, scaledRVDis, frequency, originalRV);
            frequency = this.budget;
        }

    }

    private void randomRoundEffiTwoKey(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis) {
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDis.entrySet()) {
            conScaledRVDis.put(entry.getKey(), entry.getValue());
        }
        ConcurrentHashMap<ArrayList<Integer>, Integer> fk1IdSet = new ConcurrentHashMap<>();
        ConcurrentHashMap<ArrayList<Integer>, Integer> fk2IdSet = new ConcurrentHashMap<>();
        computeFKidSet(fk1IdSet, fk2IdSet);

        String table1 = this.referenceTable.get(curTable).get(0).sourceTable;
        String table2 = this.referenceTable.get(curTable).get(1).sourceTable;
        ComKey ck1 = referenceTable.get(curTable).get(0);
        ComKey ck2 = referenceTable.get(curTable).get(1);

        long uniqBound = Integer.MAX_VALUE;
        starterRandom = 0;
        for (ArrayList<Integer> first : fk1IdSet.keySet()) {
            for (ArrayList<Integer> second : fk2IdSet.keySet()) {
                if (!ckJDAvaCounts.get(ck1).containsKey(first) || ckJDAvaCounts.get(ck1).get(first) == 0) {
                    break;
                }
                if (!ckJDAvaCounts.get(ck2).containsKey(second) || ckJDAvaCounts.get(ck2).get(second) == 0) {
                    fk2IdSet.remove(second);
                    continue;
                }

                int avaCount = Math.min(ckJDAvaCounts.get(ck1).get(first), ckJDAvaCounts.get(ck2).get(second));

                uniqBound = computeUniqBound(table1, table2, first, second);

                ArrayList<ArrayList<Integer>> rv = new ArrayList<>();
                rv.add(first);
                rv.add(second);
                int usedFrequency = processNewEdges(avaCount, rv, conScaledRVDis, uniqBound);

                if (usedFrequency == 0) {
                    continue;
                }

                updateDistribution(rv, usedFrequency);
            }

        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : conScaledRVDis.entrySet()) {
            scaledRVDis.put(entry.getKey(), entry.getValue());
        }
    }

    private void randomSwapEffi(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis) {
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDis.entrySet()) {
            conScaledRVDis.put(entry.getKey(), entry.getValue());
        }
        ConcurrentHashMap<ArrayList<Integer>, Integer> fk1IdSet = new ConcurrentHashMap<>();
        ConcurrentHashMap<ArrayList<Integer>, Integer> fk2IdSet = new ConcurrentHashMap<>();

        computeFKidSet(fk1IdSet, fk2IdSet);

        starterRandom = 0;
        for (ArrayList<Integer> first : fk1IdSet.keySet()) {
            for (ArrayList<Integer> second : fk2IdSet.keySet()) {
                if (!ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).containsKey(first) || ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).get(first) == 0) {
                    break;
                }
                if (!ckJDAvaCounts.get(referenceTable.get(curTable).get(1)).containsKey(second) || ckJDAvaCounts.get(referenceTable.get(curTable).get(1)).get(second) == 0) {
                    fk2IdSet.remove(second);
                    continue;
                }
                int avaCount = Math.min(ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).get(first), ckJDAvaCounts.get(referenceTable.get(curTable).get(1)).get(second));

                for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV : conScaledRVDis.entrySet()) {
                    if (originalRV.getValue() == 0 || originalRV.getKey().size() == 0) {
                        break;
                    }

                    ArrayList<ArrayList<Integer>> rv = new ArrayList<>();
                    rv.add(first);
                    rv.add(second);

                    avaCount = loopPairs(rv, originalRV, 2, avaCount, conScaledRVDis);

                    if (avaCount == 0) {
                        break;
                    }
                }

            }

        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : conScaledRVDis.entrySet()) {
            scaledRVDis.put(entry.getKey(), entry.getValue());
        }
    }

    private void randomRoundEffiOneKey(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis) {

        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDis.entrySet()) {
            conScaledRVDis.put(entry.getKey(), entry.getValue());
        }
        ConcurrentHashMap<ArrayList<Integer>, Integer> fk1IdSet = new ConcurrentHashMap<>();

        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            for (ArrayList<Integer> key : ckJDAvaCounts.get(referenceTable.get(curTable).get(i)).keySet()) {
                if (i == 0) {
                    fk1IdSet.put(key, 1);
                }
            }
        }

        for (ArrayList<Integer> first : fk1IdSet.keySet()) {
            if (!ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).containsKey(first)
                    || ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).get(first) == 0) {
                break;
            }
            int avaCount = ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).get(first);

            ArrayList<ArrayList<Integer>> rv = new ArrayList<>();
            rv.add(first);
            int old = 0;
            if (conScaledRVDis.containsKey(rv)) {
                old = conScaledRVDis.get(rv);
            }
            conScaledRVDis.put(rv, old + avaCount);
            ckJDAvaCounts.get(referenceTable.get(curTable).get(0)).remove(first);
        }

        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : conScaledRVDis.entrySet()) {
            scaledRVDis.put(entry.getKey(), entry.getValue());
        }
    }

    private int findUniqBound(ArrayList<ArrayList<Integer>> preVdegrees) {
        int bound1 = 1;

        if (this.allBounds.containsKey(preVdegrees)) {
            bound1 = this.allBounds.get(preVdegrees);
        } else if (preVdegrees.size() == 1) {
            bound1 = Integer.MAX_VALUE;
        } else {
            for (int t = 0; t < preVdegrees.size(); t++) {
                String sourceTable = this.referenceTable.get(curTable).get(t).sourceTable;
                if (bound1 >= Integer.MAX_VALUE / scaledJDDis.get(this.mergedDegreeTitle.get(sourceTable)).get(preVdegrees.get(t))) {
                    bound1 = Integer.MAX_VALUE;
                } else {
                    bound1 *= scaledJDDis.get(this.mergedDegreeTitle.get(sourceTable)).get(preVdegrees.get(t));
                }
            }
            allBounds.put(preVdegrees, bound1);
        }
        return bound1;
    }

    private int processNewEdges(int avaCount, ArrayList<ArrayList<Integer>> rv,
            ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis,
            long uniqBound) {
        int usedFrequency = 0;
        if (conScaledRVDis.containsKey(rv) && conScaledRVDis.get(rv) < uniqBound) {
            int oldFreq = conScaledRVDis.get(rv);
            usedFrequency = (int) Math.min(avaCount, uniqBound - oldFreq);
            if (this.eveNum && checkEQ(rv.get(0), rv.get(1))) {
                usedFrequency = usedFrequency / 2;
            }
            conScaledRVDis.put(rv, oldFreq + usedFrequency);
        } else if (!conScaledRVDis.containsKey(rv)) {
            usedFrequency = (int) Math.min(avaCount, uniqBound);
            if (this.eveNum && this.checkEQ(rv.get(0), rv.get(1))) {
                usedFrequency = usedFrequency / 2;
            }
            conScaledRVDis.put(rv, usedFrequency);
        }

        if (conScaledRVDis.get(rv) == this.allBounds.get(rv)) {
            this.allBounds.remove(rv);
            boundTrash.add(rv);
        }
        return usedFrequency;
    }

    private void updateDegrees(ArrayList<ArrayList<Integer>> preVdegrees, int vsub) {
        for (int t = 0; t < preVdegrees.size(); t++) {
            int vv = ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).get(preVdegrees.get(t));
            if (vv - vsub == 0) {
                ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).remove(preVdegrees.get(t));
            } else {
                ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).put(preVdegrees.get(t), vv - vsub);
            }
        }
        return;
    }

    private boolean checkOrder(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return false;
            } else if (first.get(i) < second.get(i)) {
                return true;
            }
        }
        return true;
    }

    private boolean checkStrictGreat(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return true;
            } else if (first.get(i) < second.get(i)) {
                return false;
            }
        }
        return false;
    }

    private boolean checkStrictEqual(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        if (first.equals(second)) {
            return true;
        }
        return false;
    }

    private boolean checkStrictLess(ArrayList<ArrayList<Integer>> calDegrees) {
        ArrayList<Integer> first = calDegrees.get(0);
        ArrayList<Integer> second = calDegrees.get(1);
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) > second.get(i)) {
                return false;
            } else if (first.get(i) < second.get(i)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkEQ(ArrayList<Integer> get, ArrayList<Integer> get0) {
        for (int i = 0; i < get.size(); i++) {
            if (!get.get(i).equals(get0.get(i))) {
                return false;
            }
        }
        return true;
    }

    private int calIndexSize(ArrayList<ArrayList<ArrayList<Integer>>> rvSet, int i) {
        int size = 1;
        for (int k = i + 1; k < rvSet.size(); k++) {
            size = size * rvSet.get(k).size();
        }
        return size;
    }

    private int loopPairs(ArrayList<ArrayList<Integer>> rv, Map.Entry<ArrayList<ArrayList<Integer>>, Integer> originalRV,
            int arrSize, int avaCount, ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis) {
        for (int k = 0; k < arrSize; k++) {
            ArrayList<ArrayList<Integer>> pair1 = new ArrayList<>();
            ArrayList<ArrayList<Integer>> pair2 = new ArrayList<>();

            boolean rfa = twoElement(pair1, pair2, rv, originalRV, k, arrSize);
            if (!rfa) {
                break;
            }

            int bound1 = 1, bound2 = 1;

            if (!this.uniqueNess.get(curTable)) {
                bound1 = Integer.MAX_VALUE;
                bound2 = Integer.MAX_VALUE;
            } else {
                bound1 = findUniqBound(pair1);
                bound2 = findUniqBound(pair2);
            }

            if ((!conScaledRVDis.containsKey(pair1) || conScaledRVDis.get(pair1) < bound1)
                    && (!conScaledRVDis.containsKey(pair2) || conScaledRVDis.get(pair2) < bound2)) {

                int minValue = ivConstraint(pair1, pair2, conScaledRVDis, bound1, bound2, originalRV, avaCount);
                if (minValue <= 0) {
                    continue;
                }
                conScaledRVDis.put(originalRV.getKey(), originalRV.getValue() - minValue);
                avaCount = updateCon(pair1, pair2, minValue, avaCount, bound1, bound2, conScaledRVDis);

                updateDegrees(rv, minValue);

                break;

            }
        }

        return avaCount;
    }

    //this is computed based on the key sum
    private ArrayList<ArrayList<Integer>> compClosest(ArrayList<Integer> key, ComKey ck) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        int thresh = 10000;
        int sum = 0;
        for (int i : key) {
            sum += i;
        }
        for (int i = 1; i <= thresh; i++) {
            for (int k = -1; k <= 1; k = k + 2) {
                if (this.jdSumMap.get(ck).containsKey(sum + k * i)) {
                    return jdSumMap.get(ck).get(sum + k * i);
                }
            }
            if (min <= i) {
                return result;
            }
        }

        return result;

    }

    private ArrayList<ArrayList<Integer>> fetchSingle(int totalNum, ArrayList<ArrayList<ArrayList<Integer>>> rvSet,
            ArrayList<ArrayList<Integer>> originalRV) {

        ArrayList<ArrayList<Integer>> matchingRV = new ArrayList<>();
        while (starterRandom < totalNum) {
            matchingRV = new ArrayList<>();
            long temp = this.starterRandom;
            for (int i = 0; i < rvSet.size(); i++) {
                int indexSize = calIndexSize(rvSet, i);
                int ind = (int) (temp / indexSize);
                if (ckJDAvaCounts.get(this.referenceTable.get(this.curTable).get(i)).containsKey(rvSet.get(i).get(ind))) {
                    matchingRV.add(rvSet.get(i).get(ind));
                    temp = temp % indexSize;
                } else {
                    this.starterRandom++;
                    String srcTable = this.referenceTable.get(this.curTable).get(i).sourceTable;
                    if (mappedBestJointDegree.get(this.mergedDegreeTitle.get(srcTable)).containsKey(originalRV.get(i))) {
                        mappedBestJointDegree.get(this.mergedDegreeTitle.get(srcTable)).get(originalRV.get(i)).remove(rvSet.get(i).get(ind));
                    }
                    cleanDistanceMapD(i, rvSet.get(i).get(ind));
                    break;
                }
            }
            if (matchingRV.size() == rvSet.size()) {
                return matchingRV;
            }
        }
        return matchingRV;
    }

    private void sortJDBasedOnValue(ArrayList<ArrayList<ComKey>> comkeys) {
        Sort sort = new Sort();
        for (int i = 0; i < referenceTable.get(curTable).size(); i++) {
            ComKey ck = referenceTable.get(curTable).get(i);
            String srcTable = ck.sourceTable;
            comkeys.add(this.mergedDegreeTitle.get(srcTable));
            sortedJDs.add(sort.sortOnValueIntegerDesc(originalCoDa.jointDegreeDis.get(comkeys.get(i))));
        }
    }

    private List<Entry<ArrayList<ArrayList<Integer>>, Long>> sortRVBasedOnJDAppearance(ArrayList<ArrayList<ComKey>> comkeys) {
        HashMap<ArrayList<ArrayList<Integer>>, Long> appearance = new HashMap<>();
        for (ArrayList<ArrayList<Integer>> rv : originalCoDa.rvDis.get(curTable).keySet()) {
            long sum1 = 1;
            for (int i = 0; i < rv.size(); i++) {
                sum1 *= originalCoDa.jointDegreeDis.get(comkeys.get(i)).get(rv.get(i));
            }
            appearance.put(rv, sum1);
        }

        List<Entry<ArrayList<ArrayList<Integer>>, Long>> sorted = new Sort().sortOnKeyAppearance(appearance, comkeys, referenceTable, originalCoDa.jointDegreeDis);
        return sorted;
    }

    private boolean checkAppearBest(ArrayList<ArrayList<Integer>> originalRV) {
        boolean found = false;
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            String sourceTable = this.referenceTable.get(curTable).get(i).sourceTable;
            if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(originalRV.get(i))) {
                found = true;
            }
        }
        return found;
    }

    private int compRVSet(ArrayList<ArrayList<Integer>> originalRV, ArrayList<ArrayList<ArrayList<Integer>>> rvSet) {
        int totalNum = 1;
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            String sourceTable = this.referenceTable.get(curTable).get(i).sourceTable;

            ArrayList<ArrayList<Integer>> cloesestJDs = new ArrayList<ArrayList<Integer>>();
            if (this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).containsKey(originalRV.get(i))) {
                for (ArrayList<Integer> jd : this.mappedBestJointDegree.get(this.mergedDegreeTitle.get(sourceTable)).get(originalRV.get(i))) {
                    cloesestJDs.add(new ArrayList<Integer>(jd));
                }
            } else if (cloesestJDs.size() == 0) {
                cloesestJDs = new ArrayList<>(compClosest(originalRV.get(i), referenceTable.get(curTable).get(i)));
            }

            if (cloesestJDs.size() == 0) {
                return -1;
            }

            rvSet.add(cloesestJDs);
            totalNum *= cloesestJDs.size();
        }
        return totalNum;
    }

    private void computeFKidSet(ConcurrentHashMap<ArrayList<Integer>, Integer> fk1IdSet, ConcurrentHashMap<ArrayList<Integer>, Integer> fk2IdSet) {
        for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
            for (ArrayList<Integer> key : ckJDAvaCounts.get(referenceTable.get(curTable).get(i)).keySet()) {
                if (i == 0) {
                    fk1IdSet.put(key, 1);
                }
                if (i == 1) {
                    fk2IdSet.put(key, 1);
                }
            }
        }
    }

    private long computeUniqBound(String table1, String table2, ArrayList<Integer> first, ArrayList<Integer> second) {
        if (this.uniqueNess.get(curTable)) {
            long v1 = scaledJDDis.get(this.mergedDegreeTitle.get(table1)).get(first);
            long v2 = scaledJDDis.get(this.mergedDegreeTitle.get(table2)).get(second);
            return v1 * v2;
        }
        return Long.MAX_VALUE;
    }

    private void updateDistribution(ArrayList<ArrayList<Integer>> rv, int usedFrequency) {
        for (int t = 0; t < rv.size(); t++) {
            int oldVal = ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).get(rv.get(t));
            ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).put(rv.get(t), oldVal - usedFrequency);
        }

        for (int t = 0; t < rv.size(); t++) {
            if (checkAvaCounts(rv.get(t), t)) {
                ckJDAvaCounts.get(referenceTable.get(curTable).get(t)).remove(rv.get(t));
            }
        }
    }

    private void randomRoundEffiThreeKeys(HashMap<ArrayList<ArrayList<Integer>>, Integer> scaledRVDis) {
        ConcurrentHashMap<ArrayList<ArrayList<Integer>>, Integer> conScaledRVDis = new ConcurrentHashMap<>();
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : scaledRVDis.entrySet()) {
            conScaledRVDis.put(entry.getKey(), entry.getValue());
        }
        ArrayList<ArrayList<ArrayList<Integer>>> idSet = new ArrayList<>();
        ArrayList<String> tables = new ArrayList<>();
        ArrayList<ComKey> cks = new ArrayList<>();
        for (int i = 0; i < referenceTable.get(curTable).size(); i++) {
            idSet.add(computeFKIDList(i));
            tables.add(this.referenceTable.get(curTable).get(i).sourceTable);
            cks.add(referenceTable.get(curTable).get(i));
            System.out.println("size: " + idSet.get(i).size());
        }

        long uniqBound = Integer.MAX_VALUE;
        starterRandom = 0;
        ArrayList<Integer> indexs = new ArrayList<>(tables.size());
        for (int i = 0; i < tables.size(); i++) {
            indexs.add(0);
        }
        while (checkIndexes(indexs, idSet)) {
            System.out.println("indexes:" + indexs);
            int avaCount = calAvaCounts(indexs, idSet, cks);
            if (avaCount == 0) {
                continue;
            }
            if (avaCount < 0) {
                System.out.println("here");
                System.out.println("indexes:" + indexs);
                System.out.println(idSet.get(0).size());
                break;
            }
            uniqBound = computeUniqBound(tables, indexs, idSet);
            ArrayList<ArrayList<Integer>> rv = computeRV(indexs, idSet);
            int usedFrequency = processNewEdges(avaCount, rv, conScaledRVDis, uniqBound);
            System.out.println("frequency: " + usedFrequency);
            updateDistribution(rv, usedFrequency);
            int returnNumber = updateLeading(indexs, indexs.size() - 1, idSet);
            if (returnNumber < 0) {
                System.out.println("there");
                System.out.println("indexes:" + indexs);
                System.out.println(idSet.get(0).size());
                break;
            }
        }
        System.out.println("indexes: " + indexs + "\t" + idSet.get(0));
        for (Map.Entry<ArrayList<ArrayList<Integer>>, Integer> entry : conScaledRVDis.entrySet()) {
            scaledRVDis.put(entry.getKey(), entry.getValue());
        }
    }

    private ArrayList<ArrayList<Integer>> computeFKIDList(int index) {
        ArrayList<ArrayList<Integer>> fkIDSet = new ArrayList<>();
        //  for (int i = 0; i < this.referenceTable.get(curTable).size(); i++) {
        for (ArrayList<Integer> key : ckJDAvaCounts.get(referenceTable.get(curTable).get(index)).keySet()) {
            fkIDSet.add(key);
        }
        return fkIDSet;
    }
//}

    private boolean checkIndexes(ArrayList<Integer> indexs, ArrayList<ArrayList<ArrayList<Integer>>> idSet) {

        for (int i = 0; i < indexs.size(); i++) {
            if (indexs.get(i) < idSet.get(i).size()) {
                return true;
            }
        }
        return false;
    }

    private int calAvaCounts(ArrayList<Integer> indexs, ArrayList<ArrayList<ArrayList<Integer>>> idSet, ArrayList<ComKey> cks) {
        int minNumber = Integer.MAX_VALUE;
        for (int i = 0; i < indexs.size(); i++) {
            int index = indexs.get(i);
            ArrayList<Integer> jd = idSet.get(i).get(index);
            if (ckJDAvaCounts.get(cks.get(i)).containsKey(jd) && ckJDAvaCounts.get(cks.get(i)).get(jd) > 0) {
                minNumber = Math.min(minNumber, ckJDAvaCounts.get(cks.get(i)).get(jd));
            } else {
                //boolean end = false;

                int returnNumber = updateLeading(indexs, i, idSet);
                //idSet.get(i).remove(index);
                /*if (!end){
                    indexs.set(i, index);
                }*/

                return returnNumber;
            }
        }

        return minNumber;
    }

    private int updateLeading(ArrayList<Integer> indexs, int i, ArrayList<ArrayList<ArrayList<Integer>>> idSet) {
        for (int j = i + 1; j < indexs.size(); j++) {
            indexs.set(j, 0);
        }
        if (i < 0) {
            return 0;
        }
        if (i == 0 && indexs.get(i) >= idSet.get(i).size() - 1) {
            return -1;
        } else if (indexs.get(i) == idSet.get(i).size() - 1) {
            indexs.set(i, 0);

            return updateLeading(indexs, i - 1, idSet);
        } else {
            indexs.set(i, (indexs.get(i) + 1));
        }
        return 0;
    }

    private long computeUniqBound(ArrayList<String> tables, ArrayList<Integer> indexs, ArrayList<ArrayList<ArrayList<Integer>>> idSet) {
        if (this.uniqueNess.get(curTable)) {
            long result = 0;
            for (int i = 0; i < tables.size(); i++) {
                long v1 = scaledJDDis.get(this.mergedDegreeTitle.get(tables.get(i))).get(idSet.get(i).get(indexs.get(i)));
                result = result * v1;
            }

            return result;
        }

        return Long.MAX_VALUE;
    }

    private ArrayList<ArrayList<Integer>> computeRV(ArrayList<Integer> indexs, ArrayList<ArrayList<ArrayList<Integer>>> idSet) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        for (int i = 0; i < indexs.size(); i++) {
            result.add(idSet.get(i).get(indexs.get(i)));
        }
        return result;
    }

}
