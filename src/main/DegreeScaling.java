package main;

import db.structs.ComKey;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map.Entry;

public class DegreeScaling implements Runnable {

    HashMap<Integer, Integer> originalDegreeDis;
    int scaledEdgeSize;
    int scaledNodeSize;
    double s_n;
    ComKey comKey;
    HashMap<ComKey, HashMap<Integer, Integer>> idDegreeDistribution;
    
    public void setInitials(HashMap<Integer, Integer> originalDegreeDis, int scaledEdgeSize, int scaledNodeSize, double s_n, 
            ComKey comKey,   HashMap<ComKey, HashMap<Integer, Integer>> idDegreeDistribution) {
        this.originalDegreeDis = originalDegreeDis;
        this.scaledEdgeSize = scaledEdgeSize;
        this.scaledNodeSize = scaledNodeSize;
        this.s_n = s_n;
        this.comKey = comKey;
        this.idDegreeDistribution = idDegreeDistribution;

    }
    
    /**
     * 
     * @param frequency
     * @return expectedFrequency
     */
    private int calExpectation(double frequency) {
        int result = (int) frequency;
        if ((frequency - result) > Math.random()) {
            result++;
        }
        return result;
    }

    /**
     * This function statically setInitials the frequency by s_n, say the
     * original frequency of degree 1 is 100, then the scaled frequency will be
     * 100*s_n.
     *
     * @param originalDegreeDis
     * @param s_n (scaling ratio)
     * @return Scaled degree distribution
     */
    private HashMap<Integer, Integer> saticScale(HashMap<Integer, Integer> originalDegreeDis, double s_n) {
        HashMap<Integer, Integer> results = new HashMap<>();

        for (Entry<Integer, Integer> entry : originalDegreeDis.entrySet()) {
            int val = calExpectation(s_n * entry.getValue());
            results.put(entry.getKey(), val);
        }
        return results;
    }

    @Override
    public void run() {
        HashMap<Integer, Integer> scaleDegree = saticScale(originalDegreeDis, s_n);
        NodeAdjustment nodeAdjustment = new NodeAdjustment();
        nodeAdjustment.adjustment(scaleDegree, scaledNodeSize);

        EdgeAdjust edgeAdjust = new EdgeAdjust(System.currentTimeMillis());
        HashMap<Integer, Integer> smoothDegree = edgeAdjust.smoothDegree(scaleDegree, scaledEdgeSize, scaledNodeSize);
        idDegreeDistribution.put(comKey, smoothDegree);
    }

}
