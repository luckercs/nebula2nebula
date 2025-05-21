import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OptionsProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(OptionsProcessor.class);
    private final Options argOptions = new Options();
    private CommandLine commandLine;

    public OptionsProcessor() {
        argOptions.addOption(new Option("m", "metad", true, "nebula metad address, eg: 127.0.0.1:9559"));
        argOptions.addOption(new Option("g", "graphd", true, "nebula grapd address, eg: 127.0.0.1:9669"));
        argOptions.addOption(new Option("u", "user", true, "nebula user, eg: root"));
        argOptions.addOption(new Option("p", "pass", true, "nebula password, eg: nebula"));

        argOptions.addOption(new Option("tm", "t_metad", true, "target nebula metad address, eg: 127.0.0.2:9559"));
        argOptions.addOption(new Option("tg", "t_graphd", true, "target nebula grapd address, eg: 127.0.0.2:9669"));
        argOptions.addOption(new Option("tu", "t_user", true, "target nebula user, eg: root"));
        argOptions.addOption(new Option("tp", "t_pass", true, "target nebula password, eg: nebula"));

        argOptions.addOption(new Option("c", "csv", true, "csv dir path"));
        argOptions.addOption(new Option("d", "delimiter", true, "csv file delimiter"));
        argOptions.addOption(new Option("h", "help", true, "show help"));
    }

    public CommandLine parse(String[] args) {
        try {
            commandLine = new GnuParser().parse(argOptions, args);
//            commandLine = new DefaultParser().parse(argOptions, args);
            if (commandLine.hasOption("h")) {
                printUsage();
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println("parse cmd err: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
        return commandLine;
    }

    public void printUsage() {
        new HelpFormatter().printHelp("nebula2nebula", argOptions);
    }
}
