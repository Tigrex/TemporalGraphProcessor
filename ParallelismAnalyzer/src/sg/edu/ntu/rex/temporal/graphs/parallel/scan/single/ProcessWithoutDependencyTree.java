package sg.edu.ntu.rex.temporal.graphs.parallel.scan.single;

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
	
	private Set<Integer> vertices;
	private List<Edge> edges;

	
	private Map<Integer, List<Edge>> outgoingEdgesMap;
	private List<List<Edge>> outgoingEdgesList;
	
	private List<Edge[]> levels;
	
	
	
	public ProcessWithoutDependencyTree() {
	}
	
	
	private void loadFile(String fileName, String separator) {
		
		vertices = new HashSet<Integer>();
		
		edges = new ArrayList<Edge>();
		
		outgoingEdgesMap = new HashMap<Integer, List<Edge>>();

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    
		    String[] parts;
		    Integer from, to;
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
		    	
		    	from = Integer.valueOf(parts[0].trim());
		    	to = Integer.valueOf(parts[1].trim());
		    	duration = Long.valueOf(parts[2].trim());
		    	departure = Long.valueOf(parts[3].trim());
		    	
		    	edge = new Edge(new Vertex(from + ""), new Vertex(to + ""), departure, departure + duration);
		    	
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
		    	if (!outgoingEdgesMap.containsKey(from)) {
		    		List<Edge> edges = new ArrayList<Edge>();
		    		
		    		edges.add(edge);
		    		outgoingEdgesMap.put(from, edges);
		    		
		    	} else {
		    		
		    		List<Edge> edges = outgoingEdgesMap.get(from);
	    			edges.add(edge);
		    		
		    	}
		    	
		    }
		    
		    // Use list instead of map for outgoing edges
		    outgoingEdgesList = new ArrayList<List<Edge>>();
		    for (int i = 0; i < vertices.size(); i++) {
		    	List<Edge> edges = outgoingEdgesMap.get(i);
		    	
		    	if (edges != null) {
		    		outgoingEdgesList.add(edges);
		    	} else {
		    		outgoingEdgesList.add(new ArrayList<Edge>());
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
		
		// Sort outgoing edges by descending order
		for (List<Edge> edges: outgoingEdgesList) {
			Collections.sort(edges, sorter);
			Collections.reverse(edges);
		}
		
	}
	
	
	
	private Map<Edge, Integer> buildInitialDependencies() {
		
		Map<Edge, Integer> dependencies = new HashMap<Edge, Integer>();
		for (Edge e: edges) {
			dependencies.put(e, 0);
		}
		
		int count = 0;
		for (Edge edge: edges) {
			count++;
			
			if (count % 1000000 == 0) {
				System.out.println("Calculating dependencies for edge " + count + "...");
			}
			
			Set<Edge> forwardEdges = findForwardDependency(edge);
			
			if (forwardEdges == null || forwardEdges.size() == 0) {
				continue;
			}
			
			for (Edge e: forwardEdges) {
				
				int oldValue = dependencies.remove(e);
				dependencies.put(e, oldValue + 1);
				
			}
			
			
		}
	
		return dependencies;
		
	}
		
	
	private Set<Edge> findForwardDependency(Edge edge) {
		
		Vertex toVertex = edge.getTo();
		Integer to = Integer.valueOf(toVertex.getId());
		
		List<Edge> edges = outgoingEdgesList.get(to);
		
		if (edges.isEmpty()) {
			return null;
		}
		
		Set<Edge> forwardDependencies = new HashSet<Edge>();

		for (Edge e: edges) {
			// Search forward to find the first infeasible edge

			if (e.getDeparture() >= edge.getArrival()) {
				forwardDependencies.add(e);
			} else {
				break;
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
