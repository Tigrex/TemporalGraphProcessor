package sg.edu.ntu.rex.temporal.graphs.parallel.scan.general;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class TemporalGraphGenerator {
	
	public static final String SEPARATOR = ",";
	
	public static final int DURATION = 1;
	
	
	public static void main(String[] args) {
		
		String fileName = "SyntheticGraph/Scale23_Edge16.csv";
		String processedFileName = fileName + ".temporal";
		
		int maxTime = 2000;
		int avgTimeInstances = 1;
		
		Random random = new Random();
		
		try {
			
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(processedFileName), "utf-8"));
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			
		    String line;
		    String[] parts;
		    String from, to;
		    
		    int numStaticEdges = 0;
		    int numTemporalEdges = 0;
		    
		    while ((line = reader.readLine()) != null) {
		    	
		    	numStaticEdges++;
		    	if (numStaticEdges % 100000 == 0) {
					System.out.println("Processing line " + numStaticEdges + "...");
				}
		    		    	
		    	line = line.trim();
		    	parts = line.split(SEPARATOR);
		    	from = parts[0].trim();
		    	to = parts[1].trim();
		    	
		    	
		    	int maxStartTime = maxTime - DURATION;

		    	int numOfTimeInstances;
		    	if (avgTimeInstances == 1) {
		    		numOfTimeInstances = 1;
		    	} else {
		    		double sd = (avgTimeInstances - 0.0)/2;
					double d = random.nextGaussian() * sd + avgTimeInstances;
					numOfTimeInstances = (int) Math.round(d);

					if (numOfTimeInstances <= 0) {
						numOfTimeInstances = 1;
					}
		    	}
				
				Set<Integer> instances = new HashSet<Integer>();
				
				while(instances.size() < numOfTimeInstances) {
					int startTime = random.nextInt(maxStartTime + 1);
					instances.add(startTime);
				}
				
				for (int startTime: instances) {
					writer.write(from + " " + to + " " + DURATION + " " + startTime);
					writer.newLine();
				}
				
				numTemporalEdges += numOfTimeInstances;
				
		    }
		    
			System.out.println("Number of static edges: " + numStaticEdges);
			System.out.println("Number of temporal edges: " + numTemporalEdges);
			
			reader.close();
			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
		
	}
	
	
	
}

