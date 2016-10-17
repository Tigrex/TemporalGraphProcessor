package sg.edu.ntu.rex.temporal.graphs.parallel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.RuntimeErrorException;

import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;
import sg.edu.ntu.rex.temporal.graphs.entity.Edge.SorterByArrival;

public class ProcessWithoutDependencyTreeOld {
	
	Set<Vertex> vertices;
	private Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges;
	private Map<Vertex, Map<Vertex, List<Edge>>> incomingEdges;
	
	List<Edge> edges;
	
	Map<Integer, Set<Edge>> levels;
	
	
	
	public ProcessWithoutDependencyTreeOld() {
	}
	
	
	private void loadFile(String fileName, String separator) {
		
		vertices = new HashSet<Vertex>();
		
		edges = new ArrayList<Edge>();
		
		outgoingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();
		incomingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();

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
		
		// Sort incoming edges
		for (Map<Vertex, List<Edge>> map: incomingEdges.values()) {
			for (List<Edge> edges: map.values()) {
				Collections.sort(edges, sorter);
			}
		}
		
	}
	
	private Set<Edge> getFirstLevel() {
		
		Set<Edge> removedSet = new HashSet<Edge>();
		
		for (Edge e: edges) {
			Set<Edge> dependencies = findForwardDependency(e);
			if (dependencies != null) {
				removedSet.addAll(dependencies);
			}
		}

		Set<Edge> firstLevel = new HashSet<Edge>();
		firstLevel.addAll(edges);
		firstLevel.removeAll(removedSet);
		
		return firstLevel;
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
	
	
	private Set<Edge> findReverseDependency(Edge edge) {
		
		Set<Edge> reverseDependencies = new HashSet<Edge>();
		
		Vertex from = edge.getFrom();
		Map<Vertex, List<Edge>> map = incomingEdges.get(from);
		
		if (map == null) {
			return null;
		}
		
		for (List<Edge> list: map.values()) {
			for (int i = 0; i < list.size(); i++) {
				Edge previous = list.get(i);
				// Check feasible
				if (previous.getArrival() > edge.getDeparture()) {
					continue;
				}
				
				List<Edge> candidates = outgoingEdges.get(edge.getFrom()).get(edge.getTo());
				
				for (int j = 0; j < candidates.size(); j++) {
					Edge better = candidates.get(j);
					
					if (better.getDeparture() < edge.getDeparture()) {
						
						if (better.getDeparture() < previous.getArrival()) {
							break;
						}
						
					} else if (better.getDeparture() == edge.getDeparture()) {
						reverseDependencies.add(edge);
						break;
					}
					
				}
				
				
			}
		}

		return reverseDependencies;
	}
	 
	
	
	public void markLevel() {

		levels = new HashMap<Integer, Set<Edge>>();
		Set<Edge> visited = new HashSet<Edge>();
		
		int level = 1;
		
		Set<Edge> current = getFirstLevel();
		Set<Edge> next = new HashSet<Edge>();
		
		do {
			
			System.out.println("Level " + level + ": " + current.size());
			
			visited.addAll(current);
//TODO			levels.put(level, current);
			
			
			Set<Edge> candidates = new HashSet<Edge>();
			
			for (Edge edge: current) {
				
				Set<Edge> forwardDependencies = findForwardDependency(edge);
				if (forwardDependencies == null || forwardDependencies.size() == 0) {
					continue;
				}
				candidates.addAll(forwardDependencies);
					
			}

			
			for (Edge candidate: candidates) {
				
				if (visited.contains(candidate)) {
					throw new RuntimeException();
				}
				
				
				// Check if all dependencies have been visited
				boolean allVisited = true;
				
				Vertex from = candidate.getFrom();
				Map<Vertex, List<Edge>> map = incomingEdges.get(from);

				for (Vertex v: map.keySet()) {

					if (!allVisited) {
						break;
					}
					
					List<Edge> previousEdges = map.get(v);
					
					for(Edge toCheck: previousEdges) {
						
						if (testEarliestArrivalDependency(toCheck, candidate)) {
							if (!visited.contains(toCheck)) {
								allVisited = false;
								break;
							}
							
						}
						
					}
					
				}
				
				if (allVisited) {
					
					if (visited.contains(candidate)) {
						throw new RuntimeException();
					}
					next.add(candidate);
				}

				
				
			}

			
			
			
			current = next;
			next = new HashSet<Edge>();
			level++;
			
		} while (current.size() > 0);
		

		
	}

	private boolean testEarliestArrivalDependency(Edge first, Edge second) {
		
		if (!first.getTo().equals(second.getFrom())) {
			throw new RuntimeException();
		}
		
		List<Edge> edges = outgoingEdges.get(second.getFrom()).get(second.getTo());
		
		// Search forward to find the first feasible edge
		for (int i = 0; i < edges.size(); i++) {
			Edge next = edges.get(i);
			// Feasible
			if (next.getDeparture() >= first.getArrival()) {
				//The first match is the earliest-arrival edge
				if (next.getDeparture() == second.getDeparture()) {
					return true;
				} else {
					return false;
				}
			}
		}
		
		return false;
	}
	
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		ProcessWithoutDependencyTreeOld processor = new ProcessWithoutDependencyTreeOld();

		String fileName = "PreprocessorSimplifiedVertex/out.munmun_digg_reply.sim";
		
		processor.loadFile(fileName, " ");
		processor.sortAllEdges();

		processor.markLevel();
		
		
		long end = System.currentTimeMillis();
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}
