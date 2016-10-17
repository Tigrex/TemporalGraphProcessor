package sg.edu.ntu.rex.temporal.graphs.parallel.scan.single;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class TemporalGraphGenerator {
	
	public static final String SEPARATOR = ",";
	
	public static final int DURATION = 1;
	
	
	public static void main(String[] args) {
		
		String fileName = "SyntheticGraph/Scale23_Edge16.csv";
		String processedFileName = fileName + ".temporal";
		
		Map<String, Set<String>> outgoingEdges = new HashMap<String, Set<String>>();
		
		int maxTime = 2000;
		
		Random random = new Random();
		
		try {
			
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(processedFileName), "utf-8"));
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			BufferedWriter report = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(processedFileName + ".report"), "utf-8"));
			
		    String line;
		    String[] parts;
		    String from, to;
		    
		    int numStaticEdges = 0;
		    int numTemporalEdges = 0;
		    
		    int duplicateEdge = 0;
		    int selfLoopingEdge = 0;
		    
	    	int maxStartTime = maxTime - DURATION;
		    
		    while ((line = reader.readLine()) != null) {
		    	
		    	numStaticEdges++;
		    	if (numStaticEdges % 100000 == 0) {
					System.out.println("Processing line " + numStaticEdges + "...");
				}
		    		    	
		    	line = line.trim();
		    	parts = line.split(SEPARATOR);
		    	from = parts[0].trim();
		    	to = parts[1].trim();
		    	
		    	if (from.equals(to)) {
		    		selfLoopingEdge++;
		    		continue;
		    	}
		    	
		    	if(outgoingEdges.containsKey(from)) {
		    		Set<String> outVertices = outgoingEdges.get(from);
		    		
		    		if (outVertices.contains(to)) {
		    			duplicateEdge++;
		    			continue;
		    		} else {
		    			outVertices.add(to);
		    		}
		    	} else {
		    		Set<String> outVertices = new HashSet<String>();
		    		outVertices.add(to);
		    		outgoingEdges.put(from, outVertices);
		    	}
		    	
		    	
				int startTime = random.nextInt(maxStartTime + 1);
				
				writer.write(from + " " + to + " " + DURATION + " " + startTime);
				writer.newLine();
				
				numTemporalEdges ++;
				
		    }
		    
		    report.write("Number of static edges: " + numStaticEdges);
		    report.newLine();
		    report.write("Number of duplicate edges: " + duplicateEdge);
		    report.newLine();
		    report.write("Number of self looping edges: " + selfLoopingEdge);
		    report.newLine();
		    report.write("Number of temporal edges: " + numTemporalEdges);
		    report.newLine();
		    
			System.out.println("Number of static edges: " + numStaticEdges);
			System.out.println("Number of duplicate edges: " + duplicateEdge);
			System.out.println("Number of self looping edges: " + selfLoopingEdge);
			System.out.println("Number of temporal edges: " + numTemporalEdges);
			
			reader.close();
			writer.close();
			report.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
		
	}
	
	
	
}

