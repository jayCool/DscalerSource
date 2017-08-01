/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class RunningException {

    static void checkTooLongRunTime(long starttime) {
        if ((System.currentTimeMillis() - starttime) / 1000 > 2000) {
            try (PrintWriter pw = new PrintWriter(new File("exception.txt"))) {
                pw.println("Running Time Too Long(Greater Than 2000 Seconds)");
                pw.close();
                System.exit(-1);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(RunningException.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
