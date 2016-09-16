package sg.edu.ntu.rex.temporal.graphs.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class StaticGraphGenerator {
	
	public StaticGraph erdosRenyi(int numOfVertices, double connectivity) {
		
		if (numOfVertices < 1 || connectivity < 0 || connectivity > 1) {
			throw new IllegalArgumentException();
		}

		Map<Vertex, Set<Vertex>> staticEdges = new HashMap<Vertex, Set<Vertex>>();

		Random random = new Random();
		
		List<Vertex> vertices = new ArrayList<Vertex>();
		for (int i = 0; i < numOfVertices; i++) {
			Vertex v = new Vertex(i + "");
			vertices.add(v);
		}
		
		for (int i = 0; i < numOfVertices; i++) {
			Set<Vertex> vertexList = new HashSet<Vertex>();
			
			for (int j = 0; j < numOfVertices; j++) {
				
				if (i == j) {
					continue;
				}
				
				double d = random.nextDouble();
				if (d < connectivity) {
					vertexList.add(vertices.get(j));
				}
				
			}
			
			staticEdges.put(vertices.get(i), vertexList);
			
		}
		
		StaticGraph g = new StaticGraph(staticEdges);
		
		return g;
	}
	
	
	public StaticGraph wattsStrogatz(int numOfVertices, int avgDegree, double disorder) {

		if (numOfVertices < 0 || avgDegree < 1 || disorder < 0 || disorder > 1) {
			throw new IllegalArgumentException();
		}

		Map<Vertex, Set<Vertex>> staticEdges = new HashMap<Vertex, Set<Vertex>>();

		Random random = new Random();
		
		List<Vertex> vertices = new ArrayList<Vertex>();
		for (int i = 0; i < numOfVertices; i++) {
			Vertex v = new Vertex(i + "");
			vertices.add(v);
		}
		
		
		// Create ring lattice first
		int plus = avgDegree / 2;
		int minus = avgDegree - plus;
		
		for (int i = 0; i < numOfVertices; i++) {
			Vertex from = vertices.get(i);
			
			Set<Vertex> vertexSet = new HashSet<Vertex>(); 
			for (int j = 1; j <= plus; j++) {
				Vertex to = vertices.get((i + j) % numOfVertices);
				vertexSet.add(to);
			}
			
			for (int j = 1; j <= minus; j++) {
				Vertex to = vertices.get((i - j + numOfVertices) % numOfVertices);
				vertexSet.add(to);
			}
			
			staticEdges.put(from, vertexSet);
		}
		
		// Rewire
		for (int i = 0; i < numOfVertices; i++) {
			Set<Vertex> vertexSet = staticEdges.get(vertices.get(i));
			
			List<Vertex> vertexList = new ArrayList<Vertex>(vertexSet);
			
			for (int j = 0; j < avgDegree; j++) {
				
				Vertex oldVertex = vertexList.get(j);
				double d = random.nextDouble();
				
				if (d < disorder) {
					
					while(true) {
						int newConnection = random.nextInt(numOfVertices);
						if (newConnection == i) {
							continue;
						}
						
						Vertex newVertex = vertices.get(newConnection);
						if (!vertexSet.contains(newVertex)) {
							vertexSet.remove(oldVertex);
							vertexSet.add(newVertex);
							break;
						}
						
					}
					
				}
				
			}
			
		}
		
		StaticGraph graph = new StaticGraph(staticEdges);
		return graph;
	}
	
	
	
	public StaticGraph barabasiAlbert(int numOfVertices, int degreeM) {
		
		if (numOfVertices < 0 || degreeM < 0) {
			throw new IllegalArgumentException();
		}
		/***************************************************
		 * 
		 * Create a bidirectional scale-free graph
		 * Undirected edges -> two directed edges
		 * 
		 ***************************************************/
		
		int initVertices = degreeM;
		
		Random random = new Random();
		
		List<Vertex> vertices = new ArrayList<Vertex>();
		
		Map<Vertex, Set<Vertex>> staticEdges = new HashMap<Vertex, Set<Vertex>>();
		
		// Init vertices
		for (int i = 0; i < initVertices; i++) {
			Vertex v = new Vertex(i + "");
			vertices.add(v);
			staticEdges.put(v, new HashSet<Vertex>());
		}
		
		// Add a first vertex, using uniform distribution to connect edges
		Vertex first = new Vertex(vertices.size() + "");
		vertices.add(first);
		staticEdges.put(first, new HashSet<Vertex>());
		
		int count = 0;
		while (count < degreeM) {
			int i = random.nextInt(vertices.size() - 1);
			Vertex v = vertices.get(i);
			
			if (!staticEdges.get(first).contains(v)) {
				
				staticEdges.get(first).add(v);
				staticEdges.get(v).add(first);
				
				count++;
			}
			
		}
		
		
		// Keep adding until reach numOfVertices
		while(vertices.size() < numOfVertices) {
			
			
			Vertex next = new Vertex(vertices.size() + "");
			vertices.add(next);
			staticEdges.put(next, new HashSet<Vertex>());
			
			count = 0;
			while (count < degreeM) {
				
				int totalDegree = 0;
				for (int i = 0; i < vertices.size() - 1; i++) {
					
					Set<Vertex> set = staticEdges.get(vertices.get(i));
					totalDegree += set.size();
				}
				
				int luckyNum = random.nextInt(totalDegree) + 1;
				Vertex luckyVertex = null;
				
				for (int i = 0; i < vertices.size() - 1; i++) {
					luckyNum -= staticEdges.get(vertices.get(i)).size();
					
					if (luckyNum <= 0) {
						luckyVertex = vertices.get(i);
						break;
					}
				}
				
				if (!staticEdges.get(next).contains(luckyVertex)) {
					
					staticEdges.get(next).add(luckyVertex);
					staticEdges.get(luckyVertex).add(next);
					
					count++;
				}
				
			}
			
			
		}
		
		
		/*
		for (int i = 0; i < staticEdges.size(); i++) {
			
			Vertex from = vertices.get(i);
			Set<Vertex> set = staticEdges.get(from);
			
			System.out.print(from.getId() + ": ");
			for (Vertex to: set) {
				System.out.print(to.getId() + " ");
			}
			System.out.println();
			
		}
		*/
		
		StaticGraph graph = new StaticGraph(staticEdges);
		return graph;
		
	}
	
	
	
	
}
