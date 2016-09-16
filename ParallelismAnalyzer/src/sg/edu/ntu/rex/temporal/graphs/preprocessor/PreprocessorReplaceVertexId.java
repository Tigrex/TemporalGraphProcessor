package sg.edu.ntu.rex.temporal.graphs.preprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class PreprocessorReplaceVertexId {

	private Map<String, Vertex> vertices;
	private Set<Edge> edges;
	
	private boolean noLoop = true;
	private boolean noDuplicate = true;
	
	private final String inputDirectory = "CSV";
	private final String transportationDirectory = "GTFS";
	private final String outputDirectory = "PreprocessorSimplifiedVertex";
	
	public PreprocessorReplaceVertexId() {
	}
	
	public PreprocessorReplaceVertexId(boolean noLoop, boolean noDuplicate) {
		this.noLoop = noLoop;
		this.noDuplicate = noDuplicate;
	}
	
	public void loadCSV(String fileName, String separator) {
		
		this.vertices = new HashMap<String, Vertex>();
		this.edges = new HashSet<Edge>();
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".report"), "utf-8"));
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 

		int loopCount = 0;
		int duplicateCount = 0;
		
		int lineCount = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(inputDirectory + "/" + fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	
		    	lineCount++;
		    	if (lineCount % 100000 == 0) {
		    		System.out.println("Reading line " + lineCount);
		    	}
		    	
		    	line = line.trim();
		    	if (line.startsWith("%")) {
		    		lineCount--;
		    		continue;
		    	}
		    	
		    	String[] parts = line.split(separator);
		    	
		    	String fromString = parts[0].trim();
		    	String toString = parts[1].trim();
		    	Vertex from, to = null;
		    	Long duration = Long.valueOf(parts[2]);
		    	Long startingTime = Long.valueOf(parts[3]);
		    	
		    	
		    	// Check from vertex
		    	if (!vertices.containsKey(fromString)) {
		    		from = new Vertex(fromString);
		    		from.setDegree(1);
					vertices.put(fromString, from);
				} else {
					from = vertices.get(fromString);
					from.setDegree(from.getDegree() + 1);
		    	}
				
		    	// Check to vertex
				if (!vertices.containsKey(toString)) {
					to = new Vertex(toString);
					vertices.put(toString, to);
				} else {
					to = vertices.get(toString);
				}
		    	
		    	
		    	Edge edge = new Edge(from, to, startingTime , startingTime + duration);
		    	
		    	if (fromString.equals(toString)) {
		    		loopCount++;
			    	
					writer.write("Self looping edge: " + edge + "...");
					writer.newLine();
			    	
	    			if (noLoop) {
	    				continue;	
			    	}
	    		}
		    	
		    	
		    	if (edges.contains(edge)) {
		    		duplicateCount++;
		    		
			    	writer.write("Duplicate edge: " + edge + "...");
			    	writer.newLine();
			    	
	    			if (noDuplicate) {
		    			continue;
			    	}
	    		}
		    	
				// Add edge
		    	edges.add(edge);

		    }
		    
		    
		    
		    writer.write("Number of vertices: " + vertices.size());
		    writer.newLine();
		    
		    writer.write("Number of edges: " + lineCount);
		    writer.newLine();

		    writer.write("Number of valid edges: " + edges.size());
		    writer.newLine();

		    writer.write("Number of loop edges: " + loopCount);
		    writer.newLine();

		    writer.write("Number of duplicate edges: " + duplicateCount);
		    writer.newLine();
		    
		    writer.close();
		    
		

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void loadTransportation(String fileName) {
		
		this.vertices = new HashMap<String, Vertex>();
		this.edges = new HashSet<Edge>();
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".report"), "utf-8"));
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 

		int loopCount = 0;
		int duplicateCount = 0;
		int zeroCount = 0;
		
		int lineCount = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(transportationDirectory + "/" + fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	
		    	lineCount++;
		    	if (lineCount % 100000 == 0) {
		    		System.out.println("Reading line " + lineCount);
		    	}
		    	
		    	line = line.trim();
		    	if (line.startsWith("%")) {
		    		lineCount--;
		    		continue;
		    	}
		    	
		    	String[] parts = line.split(" ");
		    	
		    	String fromString = parts[0].trim();
		    	String toString = parts[1].trim();
		    	Vertex from, to = null;
		    	Long startingTime = Long.valueOf(parts[2]);
		    	Long endingTime = Long.valueOf(parts[3]);
		    	
		    	
		    	// Check from vertex
		    	if (!vertices.containsKey(fromString)) {
		    		from = new Vertex(fromString);
		    		from.setDegree(1);
					vertices.put(fromString, from);
				} else {
					from = vertices.get(fromString);
					from.setDegree(from.getDegree() + 1);
		    	}
				
		    	// Check to vertex
				if (!vertices.containsKey(toString)) {
					to = new Vertex(toString);
					vertices.put(toString, to);
				} else {
					to = vertices.get(toString);
				}
		    	
		    	
		    	Edge edge = new Edge(from, to, startingTime , endingTime);
		    	
		    	if (fromString.equals(toString)) {
		    		loopCount++;
			    	
					writer.write("Self looping edge: " + edge + "...");
					writer.newLine();
			    	
	    			if (noLoop) {
	    				continue;	
			    	}
	    		}
		    	
		    	
		    	if (edges.contains(edge)) {
		    		duplicateCount++;
		    		
			    	writer.write("Duplicate edge: " + edge + "...");
			    	writer.newLine();
			    	
	    			if (noDuplicate) {
		    			continue;
			    	}
	    		}
		    	
		    	
		    	if (startingTime.equals(endingTime)) {
		    		zeroCount++;
		    		
		    		writer.write("Zero edge: " + edge + "...");
		    		writer.newLine();

		    		continue;
		    	}
		    	
				// Add edge
		    	edges.add(edge);

		    }
		    
		    
		    
		    writer.write("Number of vertices: " + vertices.size());
		    writer.newLine();
		    
		    writer.write("Number of edges: " + lineCount);
		    writer.newLine();

		    writer.write("Number of valid edges: " + edges.size());
		    writer.newLine();

		    writer.write("Number of loop edges: " + loopCount);
		    writer.newLine();

		    writer.write("Number of duplicate edges: " + duplicateCount);
		    writer.newLine();
		    
		    writer.write("Number of zero edges: " + zeroCount);
		    writer.newLine();
		    
		    writer.close();
		    
		

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	
	public void simplifyVertexId(String fileName) {
		
		// Sort vertices in descending degree
		List<Vertex> verticesList = new ArrayList<Vertex>(vertices.values());
		Collections.sort(verticesList);
		Collections.reverse(verticesList);
		
		Map<Vertex, Integer> vertexIndexes = new HashMap<Vertex, Integer>();
		
		for (int i = 0; i < verticesList.size(); i++) {
			Vertex v = verticesList.get(i);
			vertexIndexes.put(v, i);
		}
		
		System.out.println("Start to simplify edges and write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".sim"), "utf-8"));
			
			int edgeCount = 0;
			
			for (Edge e: edges) {
				
				edgeCount++;
				if (edgeCount % 100000 == 0) {
					System.out.println("Processing edge " + edgeCount);
				}
				
				Vertex from = e.getFrom();
				Vertex to = e.getTo();
				
				int fromIndex = vertexIndexes.get(from);
				int toIndex = vertexIndexes.get(to);
				
				long duration = e.getArrival() - e.getDeparture(); 
				
				String line = fromIndex + " " + toIndex + " " + duration + " " + e.getDeparture();
				writer.write(line);
				writer.newLine();
				
			}
			
			writer.close();
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish writing to file...");
		
		
	}
	
	
	public static void main(String[] args) {
		PreprocessorReplaceVertexId p = new PreprocessorReplaceVertexId();
		
		// Social Network
//		String fileName = "digg-friends";
//		String fileName = "enron";
//		String fileName = "flickr-growth";
//		String fileName = "munmun_digg_reply";
//		String fileName = "opsahl-ucsocial";
//		String fileName = "prosper-loans";
//		String fileName = "slashdot-threads";
//		String fileName = "wikipedia-growth";
		
//		fileName = "out." + fileName;
		
//		p.loadCSV(fileName, " +|\t");
//		p.simplifyVertexId(fileName);

		
		
		
		// Transportation
		String fileName = "berlin.txt";
		p.loadTransportation(fileName);
		p.simplifyVertexId(fileName);
		
		
		
		
	}
	
}
