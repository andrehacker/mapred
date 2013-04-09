
import eu.stratosphere.pact.client.nephele.api.*;
import eu.stratosphere.nephele.configuration.*;
import java.lang.System;
import java.io.File;

public class Runner {

    public static void main(String[] args) {

	String outputDir = "wordcount-result";
	String inputFile = "/../testdata/books";

	//System.out.println("Classpath: " + System.getProperty("java.class.path"));
	String pactHome = "/home/andre/dev/stratosphere/stratosphere-0.2";
	File jar = new File(pactHome + "/examples/pact/pact-examples-0.2-WordCount.jar");

	String jarDir = (new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParent();
	System.out.println("Directory of jar: " + jarDir);

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

}
