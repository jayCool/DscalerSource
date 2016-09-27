/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dsapara;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.Option;

/**
 *
 * @author workshop
 */
public class CommandParameter {

    /**
     * @param args the command line arguments
     */
    //  Dscaler fsq = new Dscaler(args[0]);
    //   fsq.s = Double.parseDouble(args[1]);
    //   fsq.delim = args[2];
    //  fsq.ignoreFirst = Boolean.parseBoolean(args[3]);
    @Option(name = "-i", usage = "input of the folder", metaVar = "INPUT")
    private String inStr = "D:\\Research\\DATA\\dscaler\\acmdl\\";

    @Option(name = "-i2", usage = "input of the file2", metaVar = "INPUT2")
    private String inStr2 = "";

    @Option(name = "-o", usage = "ouput of the folder", metaVar = "OUTPUT")
    private String outStr = "D:\\Research\\DATA\\dscaler\\acmdl\\out\\";

    @Option(name = "-d", usage = "Delimilter of the fields", metaVar = "MODE")
    private String delim = "\\s+";

    @Option(name = "-f", usage = "Ignore the first line", metaVar = "Thread")
    private String ignoreFirst = "0";

    @Option(name = "-static", usage = "Static scale of the database", metaVar = "StaticScale")
    private double s = 2;
    
    @Option(name = "-dynamic", usage = "Dynamic scale of the database", metaVar = "DynamicScale")
    private String dynamicAddr = "";
    
    
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    public static void main(String[] args) {

        try {
            new CommandParameter().doMain(args);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CommandParameter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void doMain(String[] args) throws FileNotFoundException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            parser.parseArgument(args);
            if (!inStr.isEmpty() && !outStr.isEmpty()) {
                Dscaler fsq = new Dscaler();
                fsq.delim = this.delim;
                if (this.ignoreFirst.equals("0")) {
                    fsq.ignoreFirst = false;
                }
                if (this.ignoreFirst.equals("1")) {
                    fsq.ignoreFirst = true;
                }
                fsq.dynamicAddr=this.dynamicAddr;
                fsq.filePath = inStr;
                fsq.outFile = this.outStr;
                fsq.s = this.s;
                try {
                    fsq.run();
                } catch (IOException ex) {
                    Logger.getLogger(CommandParameter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java SampleMain [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java SampleMain" + parser.printExample(ALL));
            return;
        }

        // this will redirect the output to the specified output
        // System.out.println(out);
    }

}
