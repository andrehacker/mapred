package com.andrehacker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;

public class Runner {
	
	private static String getDirectoryOfJar() {
		//System.out.println("Classpath: " + System.getProperty("java.class.path"));
		return (new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParent();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
		    printUsage();
		    System.exit(0);
		}
		System.out.println("Parameter file: " + args[0]);
		System.out.println("Classpath: " + System.getProperty("java.class.path"));
		System.out.println("Jar Directory" + getDirectoryOfJar());
		
		String propertyFile = args[0];
		
		// http://fuyun.org/2009/11/how-to-read-input-files-in-maven-junit/
		// http://stackoverflow.com/questions/6024353/maven-execjava-how-to-open-and-read-a-file-in-the-resources-directory

		ObjectMapper mapper = new ObjectMapper();
		try {
			Map<String, Object> properties = mapper.readValue(new File(propertyFile), Map.class);
			
			List<String> jobArgs = (ArrayList<String>)properties.get("job.args");
			String[] jobArgsArray = (String[])jobArgs.toArray(new String[jobArgs.size()]);
			
			String jobJar = (String)properties.get("job.jar");

			String configPath = (String)properties.get("system.configpath");
			
			File jar = new File(jobJar);

			try {

			    // PactProgram wraps plan related function
			    // Pass (classname and) arguments here
			    // PactProgram(File jarFile, String... args)
			    // PactProgram(File jarFile, String className, String... args)
			    PactProgram prog = new PactProgram(jar, jobArgsArray);
			    
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
			    // unfortunately jobDuration returned from submitJobAndWait() is not returned
			    // probably because it is only returned if we wait for completion
			    client.run(prog, true);
			    System.out.println("Job submitted succesfully");

			} catch (ProgramInvocationException e) {
			    System.out.println(e.toString());
			} catch (ErrorInPlanAssemblerException e) {
			    System.out.println(e.toString());
			}


		} catch (JsonParseException e) {
			System.out.println(e.getMessage());
		} catch (JsonMappingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}
	
    private static void printUsage() {
    	System.out.println("Error: no file with settings specified");
    }
    
    private static void printFolderContent() {
    	File cur = new File(".");
		for (File entry : cur.listFiles()) {
			System.out.println(entry.getName());
		}
    }


}
