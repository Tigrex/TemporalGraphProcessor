package sg.edu.ntu.rex.temporal.graphs.analyzer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class DiameterAnalyzer {
	
	private static final boolean DEBUG = true;
	
	private Set<Vertex> vertices;

	private Map<Vertex, Set<Vertex>> outgoingEdges;
	
	
	public void loadOutgoingEdgesCSV(String path, String separator) {
		
		vertices = new HashSet<Vertex>();
		
		outgoingEdges = new HashMap<Vertex, Set<Vertex>>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    
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
		    		
		    		Set<Vertex> outgoingVertices = new HashSet<Vertex>();
		    		outgoingVertices.add(to);
		    		
		    		outgoingEdges.put(from, outgoingVertices);
		    		
		    	} else {

		    		Set<Vertex> outgoingVertices = outgoingEdges.get(from);
		    		outgoingVertices.add(to);
		    	}
		    	
		    }
		    
			System.out.println("Number of vertices: " + vertices.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int totalEdges = 0;
		for (Vertex from: this.outgoingEdges.keySet()) {
			totalEdges += this.outgoingEdges.get(from).size();
		}
		
		System.out.println("Total number of static edges: " + totalEdges);
		
	}
	
	private int bfs(Vertex start) {
		
		int level = 0;
		
		Set<Vertex> visited = new HashSet<Vertex>();
		
		Set<Vertex> current = new HashSet<Vertex>();
		current.add(start);
		
		Set<Vertex> next = new HashSet<Vertex>();
		
		do {
			
			level++;
			next.clear();
			
			visited.addAll(current);
			
			for (Vertex v: current) {
				
				if (this.outgoingEdges.get(v) == null) {
					continue;
				}
				
				for (Vertex nextV: this.outgoingEdges.get(v)) {
					
					if (!visited.contains(nextV)) {
						next.add(nextV);
					}
				}
				
			}
			
			current.clear();
			current.addAll(next);
			
		} while (current.size() > 0);
		
		
		return level;
	}
	
	
	public int findDiameter() {
		
		int diameter = 0;
		
		for (Vertex v: this.vertices) {
			int d = bfs(v);
			
			if (d > diameter) {
				diameter = d;
			}
			
		}
		
		return diameter;
		
	}
	
	
	
	public static void main(String[] args) {
		
		DiameterAnalyzer analyzer = new DiameterAnalyzer();

		
		// Social Network
//		String fileName = "digg-friends";
//		String fileName = "enron";
		String fileName = "munmun_digg_reply";
//		String fileName = "opsahl-ucsocial";
//		String fileName = "prosper-loans";
//		String fileName = "slashdot-threads";
//		String fileName = "wikipedia-growth";
		
		
		analyzer.loadOutgoingEdgesCSV("CSV/out." + fileName, " +|\t");
		
		System.out.println("Diameter: " + analyzer.findDiameter());
		
	}

}
