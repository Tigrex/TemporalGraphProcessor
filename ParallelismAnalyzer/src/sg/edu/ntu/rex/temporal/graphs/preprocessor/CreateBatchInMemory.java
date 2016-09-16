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
import sg.edu.ntu.rex.temporal.graphs.entity.Edge.SorterByArrival;

public class CreateBatchInMemory {
	
	private Set<Vertex> vertices;
	private Set<Edge> edges;

	private Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges;
	private Map<Vertex, Map<Vertex, List<Edge>>> incomingEdges;

	private Map<Edge, List<Edge>> dependencyDAG;
	private Map<Edge, Set<Edge>> reverseDAG;
	
	private List<Set<Edge>> batches;
	
	
	private final String inputDirectory = "PreprocessorSimplifiedVertex";
	private final String outputDirectory = "OutputBatches";
	
	
	public void processFile(String fileName, String separator) throws IOException {
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".report"), "utf-8"));
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		
		long start, end;
		
		// File loading
		System.out.println("Start to load file...");
		start = System.currentTimeMillis();
		loadFile(fileName, separator);
		end = System.currentTimeMillis();
		System.out.println("Finish loading file...");
		writer.write("File loading time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		writer.write("Total number of vertices: " + vertices.size());
		writer.newLine();
		writer.write("Total number of edges: " + edges.size());
		writer.newLine();
		
		// Edge sorting
		System.out.println("Start to sort all edges...");
		start = System.currentTimeMillis();
		sortAllEdges();
		end = System.currentTimeMillis();
		System.out.println("Finish loading file...");
		writer.write("Sorting edge time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		// Dependency building
		System.out.println("Start to build dependencies...");
		start = System.currentTimeMillis();
		buildDependencyFromBothSides();
		end = System.currentTimeMillis();
		System.out.println("Finish building dependencies...");
		writer.write("Dependency building time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		int numOfDependencies = 0;
		for (List<Edge> list: dependencyDAG.values()) {
			numOfDependencies += list.size();
		}
		writer.write("Number of edges in the dependency tree: " + numOfDependencies);
		writer.newLine();

		// Create batches
		System.out.println("Start to create batches...");
		start = System.currentTimeMillis();
		createBatchesFromDependencies();
		end = System.currentTimeMillis();
		System.out.println("Finish creating batches...");
		writer.write("Batch creating time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		writer.write("Total number of batches: " + batches.size());
		writer.newLine();
		
		int total = 0;
		for (Set<Edge> batch: batches) {
			total += batch.size();
		}
		writer.write("Total number of edges: " + total);
		writer.newLine();
		
		// Write batches
		System.out.println("Start to write batches...");
		start = System.currentTimeMillis();
		writeBatchesToFile(fileName);
		end = System.currentTimeMillis();
		System.out.println("Finish writing batches...");
		writer.write("Batch writing time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		writer.close();
	}
	
	
	private void loadFile(String fileName, String separator) {
		
		vertices = new HashSet<Vertex>();
		edges = new HashSet<Edge>();
		
		outgoingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();
		incomingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();

		try (BufferedReader br = new BufferedReader(new FileReader(inputDirectory + "/" + fileName + ".sim"))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    Long departure, duration;
		    Edge edge;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 100000 == 0) {
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
		    	
				// Add edge
		    	edges.add(edge);
		    	
		    	// Update outgoing edges
		    	if (!outgoingEdges.containsKey(from)) {
		    		Map<Vertex, List<Edge>> veMap = new HashMap<Vertex, List<Edge>>();
		    		List<Edge> edges = new ArrayList<Edge>();
		    		
		    		edges.add(edge);
		    		veMap.put(to, edges);
		    		outgoingEdges.put(from, veMap);
		    		
		    	} else {
		    		Map<Vertex, List<Edge>> veMap = outgoingEdges.get(from);
		    		
		    		if (veMap.containsKey(to)) {
		    			List<Edge> edges = veMap.get(to);
		    			edges.add(edge);
		    		} else {
		    			List<Edge> edges = new ArrayList<Edge>();
		    			edges.add(edge);
		    			
		    			veMap.put(to, edges);
		    		}
		    		
		    	}
		    	
		    	// Update incoming edges
		    	if (!incomingEdges.containsKey(to)) {
		    		Map<Vertex, List<Edge>> veMap = new HashMap<Vertex, List<Edge>>();
		    		List<Edge> edges = new ArrayList<Edge>();
		    		
		    		edges.add(edge);
		    		veMap.put(from, edges);
		    		incomingEdges.put(to, veMap);

		    		
		    	} else {
		    		Map<Vertex, List<Edge>> veMap = incomingEdges.get(to);
		    		
		    		if (veMap.containsKey(from)) {
		    			List<Edge> edges = veMap.get(from);
		    			edges.add(edge);
		    		} else {
		    			List<Edge> edges = new ArrayList<Edge>();
		    			edges.add(edge);
		    			
		    			veMap.put(from, edges);
		    		}
		    		
		    	}
		    	
		    }
		    
			System.out.println("Number of vertices: " + vertices.size());
			System.out.println("Number of edges: " + edges.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	private void sortAllEdges() {
		
		SorterByArrival sorter = new SorterByArrival();
		
		// Sort outgoing edges
		for (Map<Vertex, List<Edge>> map: outgoingEdges.values()) {
			for (List<Edge> edges: map.values()) {
				Collections.sort(edges, sorter);
			}
		}
		
		// Sort incoming edges
		for (Map<Vertex, List<Edge>> map: incomingEdges.values()) {
			for (List<Edge> edges: map.values()) {
				Collections.sort(edges, sorter);
			}
		}
		
	}
	
	
	private void buildDependencyFromBothSides() {
		
		dependencyDAG = new HashMap<Edge, List<Edge>>();
		reverseDAG = new HashMap<Edge, Set<Edge>>();
		
		int count = 0;
		
		for (Vertex u: outgoingEdges.keySet()) {
			count++;
			
			if (count % 10000 == 0) {
				System.out.println("Calculating dependencies for vertex " + count + "...");
			}
			
			for (Vertex v: outgoingEdges.get(u).keySet()) {
				List<Edge> leftEdges = outgoingEdges.get(u).get(v);
				
				if (outgoingEdges.get(v) == null) {
					continue;
				}
				
				for (Vertex w: outgoingEdges.get(v).keySet()) {
					
					List<Edge> rightEdges = outgoingEdges.get(v).get(w);
					
					int pointerLeft = 0;
					int pointerRight = 0;
					
					while (pointerLeft != leftEdges.size() && pointerRight != rightEdges.size()) {
						
						Edge e1 = leftEdges.get(pointerLeft);
						Edge e2 = rightEdges.get(pointerRight);
						
						if (e1.getArrival() <= e2.getDeparture()) {
							
							addDependency(e1, e2);
							pointerLeft++;
							
						} else {
							
							pointerRight++;
							
						}
					}
					
				}
				
			}
			
		}
		
	}
	
	
	
	private void addDependency(Edge e1, Edge e2) {
		if (dependencyDAG.get(e1) != null) {
			dependencyDAG.get(e1).add(e2);
		} else {
			List<Edge> edges = new ArrayList<Edge>();
			edges.add(e2);
			dependencyDAG.put(e1, edges);
		}
		
		if (reverseDAG.get(e2) != null) {
			reverseDAG.get(e2).add(e1);
		} else {
			Set<Edge> edges = new HashSet<Edge>();
			edges.add(e1);
			reverseDAG.put(e2, edges);
		}
	}
	
	
	private void createBatchesFromDependencies() {

		batches = new ArrayList<Set<Edge>>();
		
		Set<Edge> discovered = new HashSet<Edge>();
		
		Set<Edge> firstLevel = new HashSet<Edge>();
		for (Edge e: edges) {
			if (!reverseDAG.containsKey(e)) {
				firstLevel.add(e);
			}
		}
		
		Set<Edge> current = new HashSet<Edge>();
		current.addAll(firstLevel);
		
		Set<Edge> next = new HashSet<Edge>();
		
		do {
			
			System.out.println("Level " + batches.size() + ": " + current.size());
			batches.add(current);
			discovered.addAll(current);

			next.clear();
					
			for (Edge e: current) {
				
				if (!dependencyDAG.containsKey(e)) {
					continue;
				}
				
				for (Edge edge: dependencyDAG.get(e)) {
					
					Set<Edge> edges = reverseDAG.get(edge);
					edges.remove(e);
					
					if (edges.size() == 0) {
						next.add(edge);
					}
					
				}
			}
			
			current = new HashSet<Edge>();
			current.addAll(next);
			
		} while (current.size() > 0);
		
	}
	
	
	private void writeBatchesToFile(String fileName) {
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".edges"), "utf-8"));
			
			for (Set<Edge> batch: batches) {
				for (Edge e: batch) {
					
					String line = e.getFrom().getId() + " " + e.getTo().getId() + " " + e.getDeparture() + " " + e.getArrival();
					
					writer.write(line);
					writer.newLine();
				}
			}
			
			writer.close();
			
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputDirectory + "/" + fileName + ".meta"), "utf-8"));
			
			for (Set<Edge> batch: batches) {
				String line = batch.size() + "";
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
		
	}
	
	

	public static void main(String[] args) {
		
		CreateBatchInMemory analyzer = new CreateBatchInMemory();

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
		
		String fileName = "berlin.txt";
		
		try {
			analyzer.processFile(fileName, " +|\t");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
