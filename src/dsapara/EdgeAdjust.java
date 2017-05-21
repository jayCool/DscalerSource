package dsapara;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

class EdgeAdjust extends Sort {

    long starttime = 0;

    EdgeAdjust(long currentTimeMillis) {
        this.starttime = currentTimeMillis;
    }

    /**
     * This method returns the adjustable diffs and the corresponding pairs. For
     * example, [1,2,3,4] then the pairs are {-3:, -2:, -1:, 1:, 2:, 3:}
     *
     * @param degreeList
     * @param frequencies
     * @return AdjustableDiffMap
     */
    private HashMap<Integer, ArrayList<Integer>> calAdjustableDiffAndDegreePairs(ArrayList<Integer> degreeList, ArrayList<Integer> frequencies) {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap<>();
        int maxNumber = 1000000;
        for (int i = 0; i < degreeList.size(); i++) {
            if (frequencies.get(i) > 0) {
                /*
                for (int j = i + 1; j < degreeList.size(); j++) {
                    int diff = degreeList.get(j) - degreeList.get(i);
                    if (result.containsKey(diff)) {
                        continue;
                    }
                    ArrayList<Integer> arr = new ArrayList<>();
                    arr.add(i);
                    arr.add(j);

                    result.put(degreeList.get(j) - degreeList.get(i), arr);
                }*/

                for (int j = 0; j < degreeList.size(); j++) {
                    if (result.size() > maxNumber) {
                        return result;
                    }
                    int diff = degreeList.get(j) - degreeList.get(i);
                    if (result.containsKey(diff)) {
                        continue;
                    }
                    ArrayList<Integer> arr = new ArrayList<>(2);
                    arr.add(i);
                    arr.add(j);
                    result.put(diff, arr);

                }
            }
        }
        return result;
    }

    private void test(ArrayList<Integer> frequencies, ArrayList<Integer> degreeList) {
        HashMap<Integer, Integer> positiveHashMap = new HashMap<>();
        for (int i = 0; i < degreeList.size(); i++) {
            if (frequencies.get(i) > 0) {
                positiveHashMap.put(degreeList.get(i), frequencies.get(i));
            }
        }
        System.err.println("positiveMap: " + positiveHashMap);
    }

    /**
     * This method modifies the scaledDegree to satisfy the scaledEdgeSize and
     * scaledNodeSize.
     *
     * @param scaleDegree
     * @param scaledEdgeSize
     * @param scaledNodeSize
     * @return scaledDegreeDistribution After Edge Adjustment
     * @throws FileNotFoundException
     */
    HashMap<Integer, Integer> smoothDegree(HashMap<Integer, Integer> scaleDegree, int scaledEdgeSize, int scaledNodeSize) throws FileNotFoundException {
        ArrayList<Integer> degreeList = new ArrayList<>();
        ArrayList<Integer> frequencies = new ArrayList<>();
        System.err.println("closing gap");
        System.err.println("scaledEdgeSize: " + scaledEdgeSize);
        closingDegreeGap(scaleDegree, degreeList, frequencies);
        int edgeDiff = -product(degreeList, frequencies) + scaledEdgeSize;

        scaleDegree = null;
        System.gc();
        System.err.println("Adjust Diff");
        HashMap<Integer, ArrayList<Integer>> adjustableDiffMap = calAdjustableDiffAndDegreePairs(degreeList, frequencies);
        boolean maxflag = false;

        edgeDiff = -product(degreeList, frequencies) + scaledEdgeSize;
        int ender = frequencies.size() - 1;
        int starter = 0;
        System.err.println("edgeDiff: " + edgeDiff);
        int updatedNumber = 0;
        int affectedIndex = 0;
        ArrayList<Integer> positiveIndexes = calPositiveIndexes(frequencies);
        int starterIndex = 0;
        int enderIndex = positiveIndexes.size() - 1;
        starter = positiveIndexes.get(starterIndex);
        ender = positiveIndexes.get(enderIndex);
        while (!adjustableDiffMap.containsKey(edgeDiff) && edgeDiff != 0) {
            starter = positiveIndexes.get(starterIndex);
            if (Math.random()>0.5){
                enderIndex--;
            }
            ender = positiveIndexes.get(enderIndex);

            if (starter >= ender) {
                positiveIndexes = calPositiveIndexes(frequencies);
                starterIndex = 0;
                enderIndex = positiveIndexes.size() - 1;

                continue;
            }
            RunningException.checkTooLongRunTime(starttime);
            //if ((System.currentTimeMillis() - starttime) % 100000 == 0) {
            //   test(frequencies, degreeList);

//                System.err.println("edgeDiff: " + edgeDiff + " starter: " + starter + " ender: " + ender);
            //}
            if (edgeDiff < 0) {
                if (frequencies.get(ender) > 0) {
                    frequencies.set(starter, frequencies.get(starter) + 1);
                    frequencies.set(ender, frequencies.get(ender) - 1);
                    if (frequencies.get(ender) <= 0) {
                        maxflag = true;
                        affectedIndex = ender;
                    }
                    edgeDiff += degreeList.get(ender) - degreeList.get(starter);
                    starterIndex++;
                    enderIndex--;
                } else {
                    enderIndex--;
                    continue;
                }
            } else if (frequencies.get(starter) > 0) {
                frequencies.set(starter, frequencies.get(starter) - 1);
                frequencies.set(ender, frequencies.get(ender) + 1);
                if (frequencies.get(starter) <= 0) {
                    maxflag = true;
                    affectedIndex = 0;
                }
                edgeDiff += degreeList.get(starter) - degreeList.get(ender);
                starterIndex++;
                enderIndex--;
            } else {
                starterIndex++;
                continue;
            }

            //edgeDiff = scaledEdgeSize - product(degreeList, frequencies);
            if (maxflag) {
                updatedNumber++;
                cleanAdjustTable(affectedIndex, adjustableDiffMap);
                //adjustableDiffMap = this.calAdjustableDiffAndDegreePairs(degreeList, frequencies);
                maxflag = false;
            }

            if (updatedNumber > 10) {
                adjustableDiffMap = this.calAdjustableDiffAndDegreePairs(degreeList, frequencies);
                updatedNumber = 0;
            }
        }

        if (edgeDiff != 0) {
            ArrayList<Integer> arr = adjustableDiffMap.get(edgeDiff);
            frequencies.set(arr.get(0), frequencies.get(arr.get(0)) - 1);
            frequencies.set(arr.get(1), frequencies.get(arr.get(1)) + 1);

        }

        edgeDiff = scaledEdgeSize - product(degreeList, frequencies);
        System.err.println("finished: " + edgeDiff);
        HashMap<Integer, Integer> res = new HashMap<>();

        for (int i = 0; i < degreeList.size(); i++) {
            res.put(degreeList.get(i), frequencies.get(i));
        }
        return res;
    }

    /**
     * Calculate the vector product
     *
     * @param x
     * @param value
     * @return The product of two input vector
     */
    private int product(ArrayList<Integer> x, ArrayList<Integer> value) {
        int sum = 0;
        for (int i = 0; i < x.size(); i++) {
            if (x.get(i) > 0 && value.get(i) > 0) {
                sum += x.get(i) * value.get(i);
            }
        }
        return sum;
    }

    /**
     * This method makes sure that the degreeList does not have gap in-between.
     *
     * @param scaleDegree
     * @param degreeList
     * @param frequencies
     */
    private void closingDegreeGap(HashMap<Integer, Integer> scaleDegree, ArrayList<Integer> degreeList, ArrayList<Integer> frequencies) {
        int maxDegree = Collections.max(scaleDegree.keySet());
        int minDegree = Collections.min(scaleDegree.keySet());
        for (int i = minDegree; i <= maxDegree; i++) {
            degreeList.add(i);
            frequencies.add(scaleDegree.get(i));
        }
    }

    private void cleanAdjustTable(int affectedIndex, HashMap<Integer, ArrayList<Integer>> adjustableDiffMap) {
        ArrayList<Integer> removedIndex = new ArrayList<>();
        for (Entry<Integer, ArrayList<Integer>> entry : adjustableDiffMap.entrySet()) {
            if (entry.getValue().get(0).equals(affectedIndex) || entry.getValue().get(1).equals(affectedIndex)) {
                removedIndex.add(entry.getKey());
            }
        }
        for (int index : removedIndex) {
            adjustableDiffMap.remove(index);
        }
    }

    private ArrayList<Integer> calPositiveIndexes(ArrayList<Integer> frequencies) {
        ArrayList<Integer> positive = new ArrayList<>();
        for (int i = 0; i < frequencies.size(); i++) {
            if (frequencies.get(i) > 0) {
                positive.add(i);
            }
        }
        return positive;
    }

}
