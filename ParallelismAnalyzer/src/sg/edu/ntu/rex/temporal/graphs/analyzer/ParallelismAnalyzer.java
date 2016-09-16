package sg.edu.ntu.rex.temporal.graphs.analyzer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

public class ParallelismAnalyzer {
	
	private static final boolean DEBUG = true;
	
	private Set<Vertex> vertices;
	private Set<Edge> edges;

	private Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges;
	private Map<Vertex, Map<Vertex, List<Edge>>> incomingEdges;

	private Map<Edge, Set<Edge>> dependencyDAG;
	
	
	public ParallelismAnalyzer() {
	}
	
	public void loadFile(String path, String separator) {
		
		vertices = new HashSet<Vertex>();
		edges = new HashSet<Edge>();
		
		outgoingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();
		incomingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();

		
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    Long departure, arrival;
		    Edge edge;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	line = line.trim();
		    	
		    	parts = line.split(separator);
		    	
		    	from = new Vertex(parts[0].trim());
		    	to = new Vertex(parts[1].trim());
		    	departure = Long.valueOf(parts[2].trim());
		    	arrival = Long.valueOf(parts[3].trim());
		    	
		    	edge = new Edge(from, to, departure, arrival);
		    	
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
	
	
	
	public void sortAllEdges() {
		System.out.println("Start to sort edges...");

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
		
		System.out.println("Finish sorting edges...");
		
	}
	
	
	
	public void buildDependencyTreeFromBothSides() {
		
		dependencyDAG = new HashMap<Edge, Set<Edge>>();
		
		long startTime = System.currentTimeMillis();
		System.out.println();
		System.out.println("Start to build dependencies from both sides...");
		
		int count = 0;
		
		for (Vertex u: outgoingEdges.keySet()) {
			count++;
			
			if (count % 1000 == 0 && DEBUG) {
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
					Edge essential = null;
					
					while (pointerLeft != leftEdges.size() && pointerRight != rightEdges.size()) {
						
						Edge e1 = leftEdges.get(pointerLeft);
						Edge e2 = rightEdges.get(pointerRight);
						
						if (e1.getArrival() > e2.getDeparture()) {
							if (essential != null) {
								// essential -> e2
								addDependency(essential, e2);
							}
							pointerRight++;
						} else {
							essential = e1;
							pointerLeft++;
						}
					}
					
					if (pointerLeft == leftEdges.size() && pointerRight < rightEdges.size()) {
						Edge e1 = leftEdges.get(pointerLeft - 1);
						
						while (pointerRight < rightEdges.size()) {
							Edge e2 = rightEdges.get(pointerRight);
							addDependency(e1, e2);
							pointerRight++;
						}
					}
					
				}
				
			}
			
		}
		
		System.out.println("Finish building dependencies  from both sides...");
		
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Running time (ms): " + totalTime + "...");
		
		if (DEBUG) {
			int numOfEdges = 0;
			for (Set<Edge> set: dependencyDAG.values()) {
				numOfEdges += set.size();
			}
			System.out.println("Number of edges in the dependency tree: " + numOfEdges);
			
		}
		
	}
	
	
	public void buildDependencyTreeFromBothSidesAndWrite(String fileName) throws IOException {
		
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("Temp/" + fileName + ".edge"), "utf-8"));
		
		List<Edge> edgesList = new ArrayList<Edge>(edges);
		
		Map<Edge, Integer> edgesIndex = new HashMap<Edge, Integer>();
		
		String line;
		for (int i = 0; i < edgesList.size(); i++) {
			Edge e = edgesList.get(i);
			edgesIndex.put(e, i);
			
			line = i + " " + e.getFrom().getId() + " " + e.getTo().getId() + " " + e.getDeparture() + " " + e.getArrival();
			
			writer.write(line);
			writer.newLine();
		}
		
		writer.close();
		
		writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("Temp/" + fileName + ".dep"), "utf-8"));
		
		
		long startTime = System.currentTimeMillis();
		System.out.println();
		System.out.println("Start to build dependencies from both sides...");
		
		int count = 0;
		
		
		List<Edge> leftEdges;
		List<Edge> rightEdges;
		
		
		for (Vertex u: outgoingEdges.keySet()) {
			count++;
			
			if (count % 1000 == 0 && DEBUG) {
				System.out.println("Calculating dependencies for vertex " + count + "...");
			}
			
			for (Vertex v: outgoingEdges.get(u).keySet()) {
				leftEdges = outgoingEdges.get(u).get(v);
				
				if (outgoingEdges.get(v) == null) {
					continue;
				}
				
				for (Vertex w: outgoingEdges.get(v).keySet()) {
					
					rightEdges = outgoingEdges.get(v).get(w);
					
					int pointerLeft = 0;
					int pointerRight = 0;
					Edge essential = null;
					
					while (pointerLeft != leftEdges.size() && pointerRight != rightEdges.size()) {
						
						Edge e1 = leftEdges.get(pointerLeft);
						Edge e2 = rightEdges.get(pointerRight);
						
						if (e1.getArrival() > e2.getDeparture()) {
							if (essential != null) {
								// essential -> e2
								
								int index1 = edgesIndex.get(essential);
								int index2 = edgesIndex.get(e2);
								
								line = index1 + " " + index2;
								writer.write(line);
								
								writer.newLine();
								
							}
							pointerRight++;
						} else {
							essential = e1;
							pointerLeft++;
						}
					}
					
					if (pointerLeft == leftEdges.size() && pointerRight < rightEdges.size()) {
						Edge e1 = leftEdges.get(pointerLeft - 1);
						
						while (pointerRight < rightEdges.size()) {
							Edge e2 = rightEdges.get(pointerRight);
							
							int index1 = edgesIndex.get(e1);
							int index2 = edgesIndex.get(e2);
							
							line = index1 + " " + index2;
							writer.write(line);
							writer.newLine();
							
							pointerRight++;
						}
					}
					
				}
				
			}
			
		}
		
		
		writer.close();
		
		System.out.println("Finish building dependencies  from both sides...");
		
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Dependency building time (ms): " + totalTime + "...");
		
		
	}
	
	
	public void buildDependencyTreeFromOneSide() {

		dependencyDAG = new HashMap<Edge, Set<Edge>>();
		
		long startTime = System.currentTimeMillis();
		System.out.println();
		System.out.println("Start to build dependencies from one side...");
		
		int count = 0;
		
		for (Edge edge: edges) {
			count++;
			
			if (count % 10000 == 0 && DEBUG) {
				System.out.println("Calculating dependencies for edge " + count + "...");
			}
			
			Vertex from = edge.getFrom();
			
			Map<Vertex, List<Edge>> map = incomingEdges.get(from);
			
			if (map == null) {
				continue;
			}
			
			for (List<Edge> list: map.values()) {
				// Search backwards from the latest departures
				for (int j = list.size() - 1; j >= 0; j--) {
					Edge e = list.get(j);
					// Feasible
					if (e.getArrival() <= edge.getDeparture()) {
						// e happens before edge
						addDependency(e, edge);
						//The first match is the minimum dependency
						break;
					}
				}
			}
			
		}
		
		System.out.println("Finish building dependencies from one side...");
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Running time (ms): " + totalTime + "...");
		
		if (DEBUG) {
			int numOfEdges = 0;
			for (Set<Edge> set: dependencyDAG.values()) {
				numOfEdges += set.size();
			}
			System.out.println("Number of edges in the dependency tree: " + numOfEdges);
			
		}

	}
	
	
	/**
	 * When a vertex appears in multiple levels,
	 * choose the deepest one
	 */
//	public void createBatches() {
//		System.out.println();
//		System.out.println("***********+BFS modified");
//		batches = new ArrayList<Set<Edge>>();
//		
//		Set<Edge> discovered = new HashSet<Edge>();
//		
//		Set<Edge> firstLevel = new HashSet<Edge>();
//		for (Edge e: edges) {
//			if (!reverseDependencyDAG.containsKey(e)) {
//				firstLevel.add(e);
//			}
//		}
//		
//		
//		Set<Edge> current = new HashSet<Edge>();
//		current.addAll(firstLevel);
//		
//		Set<Edge> next = new HashSet<Edge>();
//		
//		do {
//			
//			System.out.println("Level " + batches.size() + ": " + current.size());
//			batches.add(current);
//			discovered.addAll(current);
//
//			next.clear();
//					
//			for (Edge e: current) {
//				
//				if (!dependencyDAG.containsKey(e)) {
//					continue;
//				}
//				
//				for (Edge edge: dependencyDAG.get(e)) {
//					
//					// Add the edge only if all its parents have been discovered
//					boolean allDiscovered = true;
//					for (Edge parent: reverseDependencyDAG.get(edge)) {
//						if (!discovered.contains(parent)) {
//							allDiscovered = false;
//							break;
//						}
//					}
//					
//					if (allDiscovered && !discovered.contains(edge)) {
//						next.add(edge);
//					}
//					
//				}
//			}
//			
//			current = new HashSet<Edge>();
//			current.addAll(next);
//			
//		} while (current.size() > 0);
//		
//		int total = 0;
//		for (Set<Edge> batch: batches) {
//			total += batch.size();
//		}
//		System.out.println("Total number of edges: " + total);
//
//		System.out.println("***********-BFS modified");
//
//	}
	
//	public void writeBatchesToFile(String fileName) {
//		System.out.println("Start to write to file...");
//		
//		try {
//			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream("Output/" + fileName + ".txt"), "utf-8"));
//			
//			for (Set<Edge> batch: batches) {
//				for (Edge e: batch) {
//					String line = e.getFrom().getSequence() + " " + e.getTo().getSequence() + " " + e.getDeparture() + " " + e.getArrival();
//					writer.write(line);
//					writer.newLine();
//				}
//			}
//			
//			writer.close();
//			
//			writer = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream("Output/" + fileName + ".meta"), "utf-8"));
//			
//			for (Set<Edge> batch: batches) {
//				String line = batch.size() + "";
//				writer.write(line);
//				writer.newLine();
//			}
//			
//			writer.close();
//			
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		System.out.println("Finish writing to file...");
//		
//	}
	
	
	private void addDependency(Edge e1, Edge e2) {
		if (dependencyDAG.get(e1) != null) {
			dependencyDAG.get(e1).add(e2);
		} else {
			Set<Edge> set = new HashSet<Edge>();
			set.add(e2);
			dependencyDAG.put(e1, set);
		}
		
	}
	
	
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		ParallelismAnalyzer analyzer = new ParallelismAnalyzer();

		// Social Network
//		String fileName = "out.munmun_digg_reply";
//		String fileName = "out.digg-friends";
//		String fileName = "berlin";
		String fileName = "out.wikipedia-growth";
		
		
		analyzer.loadFile("Output/" + fileName + ".sim", " +");

		
		// Transportation
//		String fileName = "berlin";
//		GtfsLoader gtfsLoader = null;
//		try {
//			gtfsLoader = new GtfsLoader("GTFS/" + fileName);
//		} catch (IOException e) {
//			e.printStackTrace();
//			System.exit(1);
//		}
//		analyzer.loadGTFS(gtfsLoader);
		
		
		analyzer.sortAllEdges();
		
		//analyzer.reduceGraph(3000);
		
		//analyzer.buildDependencyTreeFromOneSide();
		
//		analyzer.buildDependencyTreeFromBothSides();
		
		
		try {
			analyzer.buildDependencyTreeFromBothSidesAndWrite(fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		analyzer.createBatches();
		
//		analyzer.writeBatchesToFile(fileName);
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}
