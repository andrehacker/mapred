
import eu.stratosphere.pact.client.nephele.api.*;
import eu.stratosphere.nephele.configuration.*;
import java.util.Properties;
import java.lang.System;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;

public class Runner {

    public static void main(String[] args) {

	String propertyFile = "job-params.properties";
	Properties prop = new Properties();
	try{
	    prop.load(new FileInputStream(propertyFile));
	} catch (IOException e) {
	    System.out.println(e.getMessage());
	    System.exit(0);
	}
	int numArgs = Integer.parseInt(prop.getProperty("job.numargs", ""));
	String[] params = new String[numArgs];
	for (int i=0; i<numArgs; ++i) {
	    params[i] = prop.getProperty("job.arg" + (i+1), "");
	    System.out.println("param" + i + ": " + params[i]);
	}

	String outputDir = "wordcount-result";
	String inputFile = "/../testdata/books";

	//System.out.println("Classpath: " + System.getProperty("java.class.path"));
	String pactHome = "/home/andre/dev/stratosphere/stratosphere-0.2";
	File jar = new File(pactHome + "/examples/pact/pact-examples-0.2-WordCount.jar");

	String jarDir = getDirectoryOfJar();

	try {

	    // PactProgram wraps plan related function
	    // Pass (classname and) arguments here
	    // PactProgram(File jarFile, String... args)
	    // PactProgram(File jarFile, String className, String... args)
	    PactProgram prog = new PactProgram(jar, new String[] {
					       "4",
					       "file://" + jarDir + inputFile,
					       "file://" + jarDir + "/" + outputDir});
	    
	    // Client compiles and submits pact programs to nephele cluster
	    // Configuration is the nephele configuration
	    // - Only Jobmanager address and port are read from configuration file
	    // - Reads all files in specified directory
	    // - results in warning, because nephele-plugins.xml does not start with configuration
	    GlobalConfiguration.loadConfiguration(pactHome + "/conf/");
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
	return (new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParent();
    }

}
