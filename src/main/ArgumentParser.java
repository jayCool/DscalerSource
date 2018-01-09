/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.Option;

/**
 *
 * @author Zhang Jiangwei
 */
public class ArgumentParser {

    @Option(name = "-i", usage = "input of the folder", metaVar = "INPUT")
    private String filePath = "";

    @Option(name = "-o", usage = "ouput of the folder", metaVar = "OUTPUT")
    private String outPath = "";

    @Option(name = "-d", usage = "Delimilter of the fields", metaVar = "Delimilter")
    private String delimiter = "\t";

    @Option(name = "-f", usage = "Ignore the first line", metaVar = "IGNORE-FIRST-LINE")
    private boolean ignoreFirst = false;

    @Option(name = "-static", usage = "Static scale of the database", metaVar = "StaticScale")
    private double staticS = 2;

    @Option(name = "-dynamic", usage = "Dynamic scale of the database, input is the file", metaVar = "DynamicScale")
    private String dynamicSFile = "";

    @Option(name = "-l", usage = "leading index of the fks", metaVar = "Leading Index")
    private int leading = 0;
    
    @Option(name = "-parallel", usage = "Parallel Running (only for large database)", metaVar = "Parallel")
    private boolean parallel = false;
    
    @Option(name = "-debug", usage = "Print out information for debugging purpose", metaVar = "DEBUG")
    private boolean debug;
    


    public static void main(String[] args) throws FileNotFoundException, IOException {

        new ArgumentParser().parseParameter(args);
    }

    private  void parseParameter(String[] args) throws FileNotFoundException {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            if (!filePath.isEmpty() && !outPath.isEmpty()) {
                Dscaler dscaler = new Dscaler();
                if (delimiter.equals("t")) {
                    delimiter = "\t";
                }
                
                dscaler.setInitials(delimiter, ignoreFirst, dynamicSFile, filePath, outPath, staticS, leading);
                dscaler.setParallel(parallel);
                try {
                    dscaler.run();
                } catch (IOException ex) {
                    Logger.getLogger(Dscaler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } catch (CmdLineException e) {
            System.err.println("  Example: java SampleMain" + parser.printExample(ALL));
            return;
        }
    }

}
