import org.apache.commons.cli.*;

import java.util.Properties;

public class Driver {

    public static void main(String[] args) throws Exception {
        Options options = OptionsParser.buildOptions();
        CommandLineParser parser = new DefaultParser();

        CommandLine cmd  = parser.parse(options,args,false);
        Properties properties = OptionsParser.retrieveAllProperties(cmd,null);

        System.out.println(properties);

    }
}
