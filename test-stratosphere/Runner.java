
import eu.stratosphere.pact.client.nephele.api.*;
import eu.stratosphere.nephele.configuration.*;
import java.util.Properties;
import java.lang.System;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;

public class Runner {
    
    private static void printUsage() {
	System.out.println("Error: no file with settings specified");
    }

    public static void main(String[] args) {

	if (args.length < 1) {
	    printUsage();
	    System.exit(0);
	}
	System.out.println("Parameter file: " + args[0]);

	String propertyFile = args[0];
	Properties prop = new Properties();
	try{
	    prop.load(new FileInputStream(propertyFile));
	} catch (IOException e) {
	    System.out.println(e.getMessage());
	    System.exit(0);
	}
	int numArgs = Integer.parseInt(prop.getProperty("job.numargs", ""));
	System.out.println("Number of parameters: " + numArgs);
	String[] params = new String[numArgs];
	for (int i=0; i<numArgs; ++i) {
	    params[i] = prop.getProperty("job.arg" + (i+1), "");
	    System.out.println("param" + i + ": " + params[i]);
	}

	String jobJar = prop.getProperty("job.jar", "");

	String configPath = prop.getProperty("system.configpath","");
	File jar = new File(jobJar);

	try {

	    // PactProgram wraps plan related function
	    // Pass (classname and) arguments here
	    // PactProgram(File jarFile, String... args)
	    // PactProgram(File jarFile, String className, String... args)
	    PactProgram prog = new PactProgram(jar, params);
	    
	    // Client compiles and submits pact programs to nephele cluster
	    // Configuration is the nephele configuration
	    // - Only Jobmanager address and port are read from configuration file
	    // - Reads all files in specified directory
	    // - results in warning, because nephele-plugins.xml does not start with configuration
	    GlobalConfiguration.loadConfiguration(configPath);
 	    Configuration config = GlobalConfiguration.getConfiguration();
	    Client client = new Client(config);

	    // Client.run(...)
	    // - compiles the PactProgram to a OptimizedPlan using PactCompiler.compile()
	    // - compiles OptimizedPlan to JogGraph using JogGraphGenerator.compileJobGraph(plan)
	    // - Uses ...nephele.client.JobClient.submitJob()/submitJobAndWait() 
	    //   to submit Job to nephele
	    // unfortunatelly jobDuration returned from submitJobAndWait() is not returned
	    // probably because it is only returned if we wait for completion
	    client.run(prog, true);
	    System.out.println("Job submitted succesfully");

	} catch (ProgramInvocationException e) {
	    System.out.println(e.toString());
	} catch (ErrorInPlanAssemblerException e) {
	    System.out.println(e.toString());
	}

    }
    
    private static String getDirectoryOfJar() {
	//System.out.println("Classpath: " + System.getProperty("java.class.path"));
	return (new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParent();
    }

}
