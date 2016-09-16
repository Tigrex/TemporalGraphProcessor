package sg.edu.ntu.rex.temporal.graphs.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class StaticGraph {

	private Map<Vertex, Set<Vertex>> staticEdges;
	
	public StaticGraph(Map<Vertex, Set<Vertex>> staticEdges) {
		this.staticEdges = staticEdges;
	}

	
	public Graph toTemporalGraph(int maxTime, int numOfTimeInstances) {
		
		return toTemporalGraph(maxTime, numOfTimeInstances, 1);
		
	}
	
	/**
	 * 
	 * Generate multiple temporal instances for a given static edge.
	 * The starting time is uniformly distributed in [0, maxTime)
	 * 
	 * @param maxTime 
	 * the time range of the graph is [0, maxTime]
	 * @param avgTimeInstances
	 * the average number of time instances for a static edge, following Gaussian distribution
	 * with sd = (avgTimeInstances) / 2
	 * @param duration
	 * average duration, fixed value as for now
	 * @return
	 */
	public Graph toTemporalGraph(int maxTime, int avgTimeInstances, int duration) {
		
		if (maxTime < 1 || avgTimeInstances >= maxTime || duration >= maxTime) {
			throw new IllegalArgumentException();
		}
		
		int maxStartTime = maxTime - duration;
		Random random = new Random();
		double sd = (avgTimeInstances - 0.0)/2;
		
		Map<Vertex, Map<Vertex, List<Edge>>> edges = new HashMap<Vertex, Map<Vertex, List<Edge>>>();
		
		for (Vertex from: staticEdges.keySet()) {
			Set<Vertex> outgoingVertices = staticEdges.get(from);
			
			Map<Vertex, List<Edge>> edgesMap = new HashMap<Vertex, List<Edge>>();
			
			for (Vertex to: outgoingVertices) {
				
				double d = random.nextGaussian() * sd + avgTimeInstances;
				int numOfTimeInstances = (int) Math.round(d);

				if (numOfTimeInstances <= 0) {
					numOfTimeInstances = 1;
				}
				
				Set<Integer> instances = new HashSet<Integer>();
				while(instances.size() < numOfTimeInstances) {
					int startTime = random.nextInt(maxStartTime + 1);
					
					if (!instances.contains(startTime)) {
						instances.add(startTime);
					}
				}
				
				List<Edge> edgeList = new ArrayList<Edge>();
				for (int startTime: instances) {
					Edge e = new Edge(from, to, startTime, startTime + duration);
					edgeList.add(e);
				}
				
				edgesMap.put(to, edgeList);
				
			}
			
			edges.put(from, edgesMap);
			
		}
		
		Graph g = new Graph(edges);
		return g;
	}
	
	
	public int getNumOfEdges() {
		
		int totalNumOfEdges = 0;
		
		for (Set<Vertex> vertices: this.staticEdges.values()) {
			totalNumOfEdges += vertices.size();
		}
		
		return totalNumOfEdges;
		
	}
	
	public static void main(String[] args) {
		
		int avgTimeInstances = 4;
		Random random = new Random();
		double sd = (avgTimeInstances - 0.0)/2;
		
		for (int i = 0; i < 10; i++) {
			double d = random.nextGaussian() * sd + avgTimeInstances;
			int numOfTimeInstances = (int) Math.round(d);
			
			System.out.println(numOfTimeInstances);
		}
		
		System.out.println("-----------------------");
		int maxStartTime = 10;
		
		for (int i = 0; i < 10; i++) {
			int startTime = random.nextInt(maxStartTime + 1);
			
			System.out.println(startTime);
		}
		
	}
	
	
	
	
}
