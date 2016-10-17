package sg.edu.ntu.rex.temporal.graphs.parallel.scan.general;

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

public class ProcessWithoutDependencyTree {
	
	Set<Vertex> vertices;
	private Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges;
	
	List<Edge> edges;
	
	List<Edge[]> levels;
	
	
	
	public ProcessWithoutDependencyTree() {
	}
	
	
	private void loadFile(String fileName, String separator) {
		
		vertices = new HashSet<Vertex>();
		
		edges = new ArrayList<Edge>();
		
		outgoingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    Long departure, duration;
		    Edge edge;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0) {
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
		    	
		    }
		    
			System.out.println("Number of vertices: " + vertices.size());
			System.out.println("Number of edges: " + count);

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
		
	}
	
	
	
	private Map<Edge, Integer> buildInitialDependencies() {
		
		Map<Edge, Integer> dependencies = new HashMap<Edge, Integer>();
		for (Edge e: edges) {
			dependencies.put(e, 0);
		}
		
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
							// e1->e2
							int oldValue = dependencies.remove(e2);
							dependencies.put(e2, oldValue + 1);
							
							pointerLeft++;
							
						} else {
							
							pointerRight++;
							
						}
					}
					
				}
				
			}
			
		}
	
		return dependencies;
		
	}
		
	
	private Set<Edge> findForwardDependency(Edge edge) {

		Set<Edge> forwardDependencies = new HashSet<Edge>();
		
		Vertex to = edge.getTo();
		Map<Vertex, List<Edge>> map = outgoingEdges.get(to);
		
		if (map == null) {
			return null;
		}
		
		for (List<Edge> list: map.values()) {
			// Search forward to find the first feasible edge
			for (int i = 0; i < list.size(); i++) {
				Edge next = list.get(i);
				// Feasible
				if (next.getDeparture() >= edge.getArrival()) {
					//The first match is the earliest-arrival edge
					forwardDependencies.add(next);
					break;
				}
			}
		}

		return forwardDependencies;
	}
	
	
	
	private void assignLevel() {

		levels = new ArrayList<Edge[]>();
		
		Set<Edge> visited = new HashSet<Edge>();
		
		Map<Edge, Integer> initialDependencies = buildInitialDependencies();
		
		List<Edge> current = new ArrayList<Edge>();
		
		// Get first level
		for (Edge e: initialDependencies.keySet()) {
			if (initialDependencies.get(e) == 0) {
				current.add(e);
			}
		}
		

		
		int level = 1;
		List<Edge> next = new ArrayList<Edge>();
		
		do {
			
			System.out.println("Level " + level + ": " + current.size());

			Edge[] currentLevel = new Edge[current.size()];
			currentLevel = current.toArray(currentLevel);
			levels.add(currentLevel);
			visited.addAll(current);
			
			for (Edge edge: current) {
				
				Set<Edge> forwardDependencies = findForwardDependency(edge);
				if (forwardDependencies == null || forwardDependencies.size() == 0) {
					continue;
				}
				
				for (Edge dependentEdge: forwardDependencies) {
					int oldValue = initialDependencies.remove(dependentEdge);
					initialDependencies.put(dependentEdge, oldValue - 1);
				}
					
			}

			for (Edge e: initialDependencies.keySet()) {
				if (initialDependencies.get(e) == 0 && !visited.contains(e)) {
					next.add(e);
				}
			}
						
			
			current = next;
			next = new ArrayList<Edge>();
			level++;
			
		} while (current.size() > 0);
		
		
	}
	
	private void writeLevelsToFile(String fileName) {
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName + ".edges"), "utf-8"));
			
			for (Edge[] edges: levels) {
				for (Edge e: edges) {
					
					String line = e.getFrom().getId() + " " + e.getTo().getId() + " " + e.getDeparture() + " " + e.getArrival();
					
					writer.write(line);
					writer.newLine();
				}
			}
			
			writer.close();
			
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName + ".meta"), "utf-8"));
			
			for (Edge[] edges: levels) {
				String line = edges.length + "";
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

	
	public void processFile(String fileName, String separator) throws IOException {
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName + ".report"), "utf-8"));
			
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
		
		// Assign level
		System.out.println("Start to assign levels...");
		start = System.currentTimeMillis();
		assignLevel();
		end = System.currentTimeMillis();
		System.out.println("Finish assigning levels...");
		writer.write("Level calculation time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		writer.write("Total number of levels: " + levels.size());
		writer.newLine();
		
		int total = 0;
		for (Edge[] edges: levels) {
			total += edges.length;
		}
		writer.write("Total number of edges: " + total);
		writer.newLine();
		
		// Write batches
		System.out.println("Start to write batches...");
		start = System.currentTimeMillis();
		writeLevelsToFile(fileName);
		end = System.currentTimeMillis();
		System.out.println("Finish writing batches...");
		writer.write("Batch writing time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		writer.close();
	}

	
	
	
	public static void main(String[] args) {
		
		ProcessWithoutDependencyTree processor = new ProcessWithoutDependencyTree();

		String fileName = "SyntheticGraph/Scale22_Edge16.csv.temporal.sim";
		
		try {
			processor.processFile(fileName, " ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
