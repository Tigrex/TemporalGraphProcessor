package sg.edu.ntu.rex.temporal.graphs.generator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

public class Graph {

	private Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges;
	private Map<Vertex, Map<Vertex, List<Edge>>> incomingEdges;

	private Map<Edge, Set<Edge>> dependencyDAG;
	private Map<Edge, Set<Edge>> reverseDAG;

	private List<Set<Edge>> batches;

	
	private boolean DEBUG = true;

	public Graph(Map<Vertex, Map<Vertex, List<Edge>>> outgoingEdges) {
		this.outgoingEdges = outgoingEdges;
	}
	
	
	public int getNumOfEdges() {

		if (this.outgoingEdges != null) {
			int totalNumOfEdges = 0;
			
			for (Map<Vertex, List<Edge>> map: outgoingEdges.values()) {
				
				for (List<Edge> list: map.values()) {
					totalNumOfEdges += list.size();
				}
				
			}
			
			return totalNumOfEdges;
		}
		
		return 0;
	}
	
	
	public void writeEdgeStream(String file) {
		
		List<Edge> edges = this.createEdgeStream();
		
		System.out.println("Start to output edge stream to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + file + ".edgestream"), "utf-8"));
			
			for (Edge e: edges) {
				writer.write(e.toString());
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
		
		System.out.println("Finish writing edge stream to file...");
		
	}
	
	
	private List<Edge> createEdgeStream() {
		
		if (this.outgoingEdges != null) {
			
			System.out.println("Create edge stream from outgoing edges...");
			
			List<Edge> edges = new ArrayList<Edge>();
			
			// Add all outgoing edges
			for (Map<Vertex, List<Edge>> map: outgoingEdges.values()) {
				for (List<Edge> edgeList: map.values()) {
					edges.addAll(edgeList);
				}
			}

			SorterByArrival sorter = new SorterByArrival();
			Collections.sort(edges, sorter);
			
			return edges;
			
		} else {

			System.out.println("No edges in the graph...");
			
			throw new RuntimeException();
		}
		
	}
	
	
	private void prepareIncomingEdges() {
		this.incomingEdges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();
		
		for (Vertex from: outgoingEdges.keySet()) {
			
			Map<Vertex, List<Edge>> map = outgoingEdges.get(from);
			
			for (Vertex to: map.keySet()) {
				
				List<Edge> edges = map.get(to);
				Map<Vertex, List<Edge>> reverseMap = new HashMap<Vertex, List<Edge>>();
				reverseMap.put(from, edges);
				
				incomingEdges.put(to, reverseMap);
				
			}
			
		}
		
	}
	
	
	
	
	public void createBatches() {
		
		this.buildDependencyTreeFromBothSides();
		
		List<Edge> edgesList = this.createEdgeStream();
		
		long start = System.currentTimeMillis();

		System.out.println("***********+BFS modified");
		batches = new ArrayList<Set<Edge>>();
		
		Set<Edge> discovered = new HashSet<Edge>();
		
		Set<Edge> firstLevel = new HashSet<Edge>();
		for (Edge e: edgesList) {
			if (!reverseDAG.containsKey(e)) {
				firstLevel.add(e);
			}
		}
		
		
		Set<Edge> current = new HashSet<Edge>();
		current.addAll(firstLevel);
		
		Set<Edge> next = new HashSet<Edge>();
		
		do {
			
			// System.out.println("Level " + batches.size() + ": " + current.size());
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
		
		int total = 0;
		for (Set<Edge> batch: batches) {
			total += batch.size();
		}
		System.out.println("Total number of edges: " + total);
		
		System.out.println("Total number of batches: " + batches.size());
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		
		System.out.println("***********-BFS modified");
		
	}

	
	
	private void buildDependencyTreeFromBothSides() {

		this.prepareIncomingEdges();
		
		dependencyDAG = new HashMap<Edge, Set<Edge>>();
		reverseDAG = new HashMap<Edge, Set<Edge>>();
		
		long startTime = System.currentTimeMillis();
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

	

	private void addDependency(Edge e1, Edge e2) {
		if (dependencyDAG.get(e1) != null) {
			dependencyDAG.get(e1).add(e2);
		} else {
			Set<Edge> set = new HashSet<Edge>();
			set.add(e2);
			dependencyDAG.put(e1, set);
		}
		
		if (reverseDAG.get(e2) != null) {
			reverseDAG.get(e2).add(e1);
		} else {
			Set<Edge> set = new HashSet<Edge>();
			set.add(e1);
			reverseDAG.put(e2, set);
		}
		
	}
	
	
	
}
