package sg.edu.ntu.rex.temporal.graphs.analyzer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;

import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class TemporalAnalyzer {

	private static final boolean DEBUG = true;
	
	private Set<Vertex> vertices;

	private Map<Vertex, Map<Vertex, Set<Edge>>> outgoingEdges;
	
	
	public void loadOutgoingEdgesGTFS(String path) {
		
		this.vertices = new HashSet<Vertex>();
		outgoingEdges = new HashMap<Vertex, Map<Vertex, Set<Edge>>>();

		
		int loopCount = 0;
		int zeroCount = 0;
		
		GtfsLoader loader = null;
		try {
			loader = new GtfsLoader("GTFS/" + path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Map<Trip, List<StopTime>> trips = loader.getTrips();
		
		for (List<StopTime> stopTimes: trips.values()) {
			
			for (int i = 0; i < stopTimes.size() - 1; i++) {
				StopTime stFrom = stopTimes.get(i);
				StopTime stTo = stopTimes.get(i + 1);
				
				Vertex from = new Vertex(stFrom.getStop().getId().toString());
				Vertex to = new Vertex(stTo.getStop().getId().toString());
				
				long departureTime = (long) stFrom.getDepartureTime();
				long arrivalTime = (long) stTo.getArrivalTime();
				
		    	Edge edge = new Edge(from, to, departureTime , arrivalTime);

				if (departureTime == arrivalTime) {
					zeroCount++;
					if (DEBUG) {
				    	System.out.println("Zero cost edge: " + edge + "...");
			    	}
    				continue;	
					
				}
				
		    	if (from.equals(to)) {
		    		loopCount++;
			    	System.out.println("Self looping edge: " + edge + "...");
    				continue;	
	    		}
		    	
		    	
		    	// Update outgoing edges
		    	if (!outgoingEdges.containsKey(from)) {
		    		Map<Vertex, Set<Edge>> veMap = new HashMap<Vertex, Set<Edge>>();
		    		Set<Edge> edges = new HashSet<Edge>();
		    		
		    		edges.add(edge);
		    		veMap.put(to, edges);
		    		outgoingEdges.put(from, veMap);
		    		
		    	} else {
		    		Map<Vertex, Set<Edge>> veMap = outgoingEdges.get(from);
		    		
		    		if (veMap.containsKey(to)) {
		    			Set<Edge> edges = veMap.get(to);
		    			edges.add(edge);
		    		} else {
		    			Set<Edge> edges = new HashSet<Edge>();
		    			edges.add(edge);
		    			
		    			veMap.put(to, edges);
		    		}
		    		
		    	}
		    	
		    	
			}
			
		}
			
		System.out.println("Number of vertices: " + vertices.size());
		System.out.println("Number of loop edges: " + loopCount);
		System.out.println("Number of zero edges: " + zeroCount);
		
	}
	
	
	public void loadOutgoingEdgesCSV(String path, String separator) {
		
		vertices = new HashSet<Vertex>();
		
		outgoingEdges = new HashMap<Vertex, Map<Vertex, Set<Edge>>>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    Long departure, duration;
		    Edge edge;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	if (line.startsWith("%")) {
		    		continue;
		    	}
		    	
		    	count++;
		    	if (count % 1000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	line = line.trim();
		    	
		    	parts = line.split(separator);
		    	
		    	from = new Vertex(parts[0].trim());
		    	to = new Vertex(parts[1].trim());
		    	duration = Long.valueOf(parts[2].trim());
		    	departure = Long.valueOf(parts[3].trim());
		    	
		    	edge = new Edge(from, to, departure, departure + duration);
		    	
		    	// Check from vertex
		    	if (!vertices.contains(from)) {
					vertices.add(from);
				} 
				
		    	// Check to vertex
				if (!vertices.contains(to)) {
					vertices.add(to);
				}
		    	
		    	// Update outgoing edges
		    	if (!outgoingEdges.containsKey(from)) {
		    		Map<Vertex, Set<Edge>> veMap = new HashMap<Vertex, Set<Edge>>();
		    		Set<Edge> edges = new HashSet<Edge>();
		    		
		    		edges.add(edge);
		    		veMap.put(to, edges);
		    		outgoingEdges.put(from, veMap);
		    		
		    	} else {
		    		Map<Vertex, Set<Edge>> veMap = outgoingEdges.get(from);
		    		
		    		if (veMap.containsKey(to)) {
		    			Set<Edge> edges = veMap.get(to);
		    			edges.add(edge);
		    		} else {
		    			Set<Edge> edges = new HashSet<Edge>();
		    			edges.add(edge);
		    			
		    			veMap.put(to, edges);
		    		}
		    		
		    	}
		    	
		    }
		    
			System.out.println("Number of vertices: " + vertices.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public void writeTemporalInstancesGTFS(String fileName) {
		Map<Long, Integer> temporalInstancesMap = new HashMap<Long, Integer>();
		
		int loopCount = 0;
		int zeroCount = 0;
		
		GtfsLoader loader = null;
		try {
			loader = new GtfsLoader("GTFS/" + fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Map<Trip, List<StopTime>> trips = loader.getTrips();
		
		for (List<StopTime> stopTimes: trips.values()) {
			
			for (int i = 0; i < stopTimes.size() - 1; i++) {
				StopTime stFrom = stopTimes.get(i);
				StopTime stTo = stopTimes.get(i + 1);
				
				Vertex from = new Vertex(stFrom.getStop().getId().toString());
				Vertex to = new Vertex(stTo.getStop().getId().toString());
				
				long departureTime = (long) stFrom.getDepartureTime();
				long arrivalTime = (long) stTo.getArrivalTime();
				
		    	Edge edge = new Edge(from, to, departureTime , arrivalTime);

				if (departureTime == arrivalTime) {
					zeroCount++;
					if (DEBUG) {
				    	System.out.println("Zero cost edge: " + edge + "...");
			    	}
    				continue;	
					
				}
				
		    	if (from.equals(to)) {
		    		loopCount++;
			    	System.out.println("Self looping edge: " + edge + "...");
    				continue;	
	    		}
		    	
		    	if (temporalInstancesMap.containsKey(departureTime)) {
					int value = temporalInstancesMap.get(departureTime);
					temporalInstancesMap.remove(departureTime);
					temporalInstancesMap.put(departureTime, value + 1);
				} else {
					temporalInstancesMap.put(departureTime, 1);
				}
		    	
//		    	if (temporalInstancesMap.containsKey(arrivalTime)) {
//					int value = temporalInstancesMap.get(arrivalTime);
//					temporalInstancesMap.remove(arrivalTime);
//					temporalInstancesMap.put(arrivalTime, value + 1);
//				} else {
//					temporalInstancesMap.put(arrivalTime, 1);
//				}
		    	
		    	
			}
			
		}
			
		System.out.println("Number of loop edges: " + loopCount);
		System.out.println("Number of zero edges: " + zeroCount);
		
		
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + "_instance.txt"), "utf-8"));
			
			for (Long departure: temporalInstancesMap.keySet()) {
				int value = temporalInstancesMap.get(departure);
				
				writer.write(departure + ", " + value);
				writer.newLine();
			}
			
			writer.close();
			
			System.out.println("Total number of time instances: " + temporalInstancesMap.size());
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish writing to file...");
		
	}
	
	
	public void writeTemporalInstancesCSV(String fileName, String separator) {
		
		Map<Long, Integer> temporalInstancesMap = new HashMap<Long, Integer>();
		
		try (BufferedReader br = new BufferedReader(new FileReader("CSV/out." + fileName))) {
		    String line;
		    
		    String[] parts;
		    Long departure;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	if (line.startsWith("%")) {
		    		continue;
		    	}
		    	
		    	count++;
		    	if (count % 1000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	line = line.trim();
		    	
		    	parts = line.split(separator);
		    	departure = Long.valueOf(parts[3]);
		    	
				if (temporalInstancesMap.containsKey(departure)) {
					int value = temporalInstancesMap.get(departure);
					temporalInstancesMap.remove(departure);
					temporalInstancesMap.put(departure, value + 1);
				} else {
					temporalInstancesMap.put(departure, 1);
				}

		    }
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + "_instance.txt"), "utf-8"));
			
			for (Long departure: temporalInstancesMap.keySet()) {
				int value = temporalInstancesMap.get(departure);
				
				writer.write(departure + ", " + value);
				writer.newLine();
			}
			
			writer.close();
			
			System.out.println("Total number of time instances: " + temporalInstancesMap.size());
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish writing to file...");
		
		
		
	}
	
	
	
	public void writeTemporalDegree(String fileName) {
		
		Map<Integer, Integer> degreeMap = new HashMap<Integer, Integer>();
		
		for (Vertex from: this.outgoingEdges.keySet()) {
			
			Map<Vertex, Set<Edge>> outMap = this.outgoingEdges.get(from);
			for (Vertex to: outMap.keySet()) {
				Set<Edge> set = outMap.get(to);
				int degree = set.size();

				if (degreeMap.containsKey(degree)) {
					int count = degreeMap.get(degree);
					degreeMap.remove(degree);
					degreeMap.put(degree, count + 1);
				} else {
					degreeMap.put(degree, 1);
				}
				
			}
			
		}
			
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + ".txt"), "utf-8"));
			
			int totalEdges = 0;
			
			for (Integer degree: degreeMap.keySet()) {
				int count = degreeMap.get(degree);
				
				totalEdges += degree * count;

				writer.write(degree + ", " + count);
				writer.newLine();
			}
			
			writer.close();
			
			System.out.println("Total number of edges: " + totalEdges);
			
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
		
		long start = System.currentTimeMillis();
		TemporalAnalyzer analyzer = new TemporalAnalyzer();

		// Social Network
//		String fileName = "digg-friends";
//		String fileName = "enron";
//		String fileName = "munmun_digg_reply";
//		String fileName = "opsahl-ucsocial";
//		String fileName = "prosper-loans";
//		String fileName = "slashdot-threads";
//		String fileName = "wikipedia-growth";
		
//		analyzer.loadOutgoingEdges("CSV/out." + fileName, " +|\t");
//		analyzer.writeTemporalDegree(fileName);
//		analyzer.writeTemporalInstances(fileName, " +|\t");

		
		
		
		
		
		// GTFS transportation
		String fileName = "berlin";
		
//		analyzer.loadOutgoingEdgesGTFS(fileName);
//		analyzer.writeTemporalDegree(fileName);
		
		analyzer.writeTemporalInstancesGTFS(fileName);
		
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
		
		
		
		
	}
	
	
}
