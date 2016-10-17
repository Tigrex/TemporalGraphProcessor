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

public class ProcessOptimized {
	
	// Graph data
	private Set<Integer> vertices;
	private List<Edge> edges;

	// Outgoing data structure
	private List<List<Edge>> outgoingEdgesList;
	
	// Edges with level
	private List<Edge[]> levels;
	
	
	public ProcessOptimized() {
	}
	
	
	private void loadFile(String fileName, String separator) {
		
		vertices = new HashSet<Integer>();
		edges = new ArrayList<Edge>();
		Map<Integer, List<Edge>> outgoingEdgesMap = new HashMap<Integer, List<Edge>>();

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
		
		// Sort outgoing edges by ascending order
		for (List<Edge> edges: outgoingEdgesList) {
			Collections.sort(edges, sorter);
		}
		
	}

	private void assignEdgeIndex() {
		
		for (int i = 0; i < edges.size(); i++) {
			Edge e = edges.get(i);
			e.index = i;
		}
		
	}
	
	
	private List<Integer> buildInitialDependencies() {
		
		List<Integer> dependencies = new ArrayList<Integer>();
		
		for (int i = 0; i < edges.size(); i++) {
			dependencies.add(0);
		}
		
		for (int i = 0; i < edges.size(); i++) {
			
			Edge edge = edges.get(i);
			
			if (i % 1000000 == 0) {
				System.out.println("Calculating dependencies for edge " + i + "...");
			}
			
			
			// Find forward dependencies
			Vertex toVertex = edge.getTo();
			Integer to = Integer.valueOf(toVertex.getId());
			
			List<Edge> outEdges = outgoingEdgesList.get(to);
			
			if (outEdges.isEmpty()) {
				continue;
			}
			
			
			int index = binarySearch(edge.getArrival(), outEdges);
			
			if (index == -1) {
				// No earliest-arrival dependency
				continue;
			} else {
				
				Edge dependentE;
				for (int j = index; j < outEdges.size(); j++) {
					dependentE = outEdges.get(j);
					// edge -> dependentE
					
					// Prune unnecessary dependencies
					if (edge.getFrom().getId().equals(dependentE.getTo().getId())) {
						continue;
					}
					
					int old = dependencies.get(dependentE.index);
					dependencies.set(dependentE.index, old + 1);
					
				}
				
				
			}
			
		}
	
		return dependencies;
		
	}
		
	
	/**
	 * Returns the index of the first feasible edge e
	 * s.t. e.departure >= arrivalTime
	 * assume edges are sorted in ascending order
	 * @param deadline
	 * @param edges
	 * @return
	 */
	private int binarySearch(long arrivalTime, List<Edge> edges) {
		
		int left = 0;
		int right = edges.size() - 1;
		int middle;
		
		int candidate = -1;

		Edge temp;
		while (left <= right) {
			middle = (left + right)/2;
			temp = edges.get(middle);
			
			if (temp.getDeparture() >= arrivalTime) {
				
				if (candidate == -1 || candidate > middle) {
					candidate = middle;
				}
				right = middle - 1;
				
			} else {
				left = middle + 1;
			}
					
		}
		
		return candidate;
	}
	
	
	
	private void assignLevel(List<Integer> initialDependencies) {

		levels = new ArrayList<Edge[]>();
		
		// Get first level
		List<Edge> current = new ArrayList<Edge>();
		for (int i = 0; i < edges.size(); i++) {
			if (initialDependencies.get(i).equals(0)) {
				current.add(edges.get(i));
			}
		}
		
		
		int level = 1;
		List<Edge> next = new ArrayList<Edge>();
		
		do {
			
			System.out.println("Level " + level + ": " + current.size());

			// Store current level
			Edge[] currentLevel = new Edge[current.size()];
			currentLevel = current.toArray(currentLevel);
			levels.add(currentLevel);
			
			
			next = new ArrayList<Edge>();
			for (Edge edge: current) {
				
				
				Vertex toVertex = edge.getTo();
				Integer to = Integer.valueOf(toVertex.getId());
				
				List<Edge> outEdges = outgoingEdgesList.get(to);
				
				if (outEdges.isEmpty()) {
					continue;
				}
				
				int index = binarySearch(edge.getArrival(), outEdges);
				
				
				if (index == -1) {
					// No earliest-arrival dependency
					continue;
				} else {
					
					Edge dependentE;
					for (int i = index; i < outEdges.size(); i++) {
						dependentE = outEdges.get(i);
						// edge -> dependentE
						
						int old = initialDependencies.get(dependentE.index);
						initialDependencies.set(dependentE.index, old - 1);
						
						// All previous dependencies have been met
						if (old - 1 == 0) {
							next.add(dependentE);
						}
						
					}
					
					
				}
				
					
			}

			current = next;
			level++;
			
		} while (current.size() > 0);
		
		int totalEdges = 0;
		for (Edge[] edges: levels) {
			totalEdges += edges.length;
		}
		if (totalEdges != edges.size()) {
			System.out.println("Number of edges in temporal graph: " + edges.size());
			System.out.println("Number of edges in dependency graph: " + totalEdges);
			throw new RuntimeException("Total number of edges do not match...");
		}
		
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
		
		// Assign edge index
		System.out.println("Start to assign edge index...");
		start = System.currentTimeMillis();
		assignEdgeIndex();
		end = System.currentTimeMillis();
		System.out.println("Finish assigning index...");
		writer.write("Assigning edge index time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		
		// Build initial dependencies
		System.out.println("Start to build initial dependencies...");
		start = System.currentTimeMillis();
		List<Integer> initialDependencies = buildInitialDependencies();
		end = System.currentTimeMillis();
		System.out.println("Finish building initial dependencies...");
		writer.write("Build initial dependencies time: " + (end-start)*1.0/1000 + " seconds.");
		writer.newLine();
		
		
		// Assign level
		System.out.println("Start to assign levels...");
		start = System.currentTimeMillis();
		assignLevel(initialDependencies);
		end = System.currentTimeMillis();
		System.out.println("Finish assigning levels...");
		writer.write("Level assigning time: " + (end-start)*1.0/1000 + " seconds.");
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
		
		ProcessOptimized processor = new ProcessOptimized();

		String fileName = "SyntheticGraph/Scale23_Edge16.csv.temporal.sim";
		
		try {
			processor.processFile(fileName, " ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
