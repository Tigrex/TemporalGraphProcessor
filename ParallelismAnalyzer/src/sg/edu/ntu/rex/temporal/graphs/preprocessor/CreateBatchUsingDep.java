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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class CreateBatchUsingDep {
	
	private static final boolean DEBUG = true;
	
	private List<Edge> edgesList;

	private Map<Integer, Set<Integer>> dependencyDAG;
	private Map<Integer, Set<Integer>> reverseDAG;
	
	
	private List<List<Integer>> dependencyList;
	private List<Set<Integer>> reverseList;
	
	
	private Set<Integer> firstLevel;
	
	List<Set<Integer>> batches;
	
	
	public CreateBatchUsingDep() {
	}
	
	public void loadEdges(String file) {
		
		edgesList = new ArrayList<Edge>();
		
		System.out.println("Start to load edge file.");
		
		try (BufferedReader br = new BufferedReader(new FileReader("Temp/" + file + ".edge"))) {
		    String line;
		    
		    String[] parts;
		    Vertex from, to;
		    Long departure, arrival;
		    Edge edge;
		    
		    long count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	line = line.trim();
		    	
		    	parts = line.split(" ");
		    	
		    	from = new Vertex(parts[1].trim());
		    	to = new Vertex(parts[2].trim());
		    	departure = Long.valueOf(parts[3].trim());
		    	arrival = Long.valueOf(parts[4].trim());
		    	
		    	edge = new Edge(from, to, departure, arrival);
		    	
		    	
				// Add edge
		    	edgesList.add(edge);
		    	
		    }
		    
			System.out.println("Number of edges: " + edgesList.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish loading edge file.");
		
	}
	
	public void loadDependencyUsingList(String file) {
		loadDependencyUsingList(file, -1);
	}
	
	
	public void loadDependencyUsingList(String file, int numOfEdges) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load dependency file.");
		
		dependencyList = new ArrayList<List<Integer>>();
		reverseList = new ArrayList<Set<Integer>>();
		
		if (numOfEdges == -1) {
			for (int i = 0; i < edgesList.size(); i++) {
				dependencyList.add(new ArrayList<Integer>());
				reverseList.add(new HashSet<Integer>());
			}
		} else {
			for (int i = 0; i < numOfEdges; i++) {
				dependencyList.add(new ArrayList<Integer>());
				reverseList.add(new HashSet<Integer>());
			}
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader("Temp/" + file + ".dep"))) {
		    String line;
		    
		    String[] parts;
		    int from, to;
		    
		    long count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
//		    	line = line.trim();
		    	
		    	parts = line.split(" ");
		    	
		    	from = Integer.valueOf(parts[0]);
		    	to = Integer.valueOf(parts[1]);
		    	
		    	// Update dependency graph
		    	dependencyList.get(from).add(to);
		    	
		    	// Update reverse graph
		    	reverseList.get(to).add(from);
		    	
		    }
		    
			System.out.println("Number of dependencies: " + count);

			
			/*
			for (int i = 0; i < edgesMap.size(); i++) {
				List<Integer> incoming = dependencyList.get(i);
				List<Integer> outgoing = reverseList.get(i);
				
				Set<Integer> inSet = new HashSet<Integer>(incoming);
				Set<Integer> outSet = new HashSet<Integer>(outgoing);
				
				if (incoming.size() != inSet.size()) {
					System.out.println("Error");
				}
				
				if (outgoing.size() != outSet.size()) {
					System.out.println("Error 2");
				}
				
				
				
			}
			*/
			
			
			
			
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		System.out.println("Finish loading dependency file.");
		
	}
	
	
	public void loadDependency(String file) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load dependency file.");
		
		dependencyDAG = new HashMap<Integer, Set<Integer>>();
		reverseDAG = new HashMap<Integer, Set<Integer>>();
		
		try (BufferedReader br = new BufferedReader(new FileReader("Temp/" + file + ".dep"))) {
		    String line;
		    
		    String[] parts;
		    int from, to;
		    
		    int count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	line = line.trim();
		    	
		    	parts = line.split(" ");
		    	
		    	from = Integer.valueOf(parts[0].trim());
		    	to = Integer.valueOf(parts[1].trim());
		    	
		    	// Update dependency graph
		    	if (dependencyDAG.containsKey(from)) {
		    		Set<Integer> set = dependencyDAG.get(from);
		    		set.add(to);
		    	} else {
		    		Set<Integer> set = new HashSet<Integer>();
		    		set.add(to);
		    		dependencyDAG.put(from, set);
		    	}
		    	
		    	// Update reverse graph
		    	if (reverseDAG.containsKey(to)) {
		    		Set<Integer> set = reverseDAG.get(to);
		    		set.add(from);
		    	} else {
		    		Set<Integer> set = new HashSet<Integer>();
		    		set.add(from);
		    		reverseDAG.put(to, set);
		    	}
		    }
		    
			System.out.println("Number of dependencies: " + count);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		System.out.println("Finish loading dependency file.");
		
	}

	
	
	
	
	
	public void loadDependencyHalf(String file, int numOfEdges) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load dependency file.");
		
		dependencyList = new ArrayList<List<Integer>>();
		for (int i = 0; i < numOfEdges; i++) {
			dependencyList.add(new ArrayList<Integer>());
		}
		
		firstLevel = new HashSet<Integer>();
		
		for (int i = 0; i < numOfEdges; i++) {
			firstLevel.add(i);
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader("Temp/" + file + ".dep"))) {
		    String line;
		    
		    String[] parts;
		    int from, to;
		    
		    long count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	parts = line.split(" ");
		    	
		    	from = Integer.valueOf(parts[0]);
		    	to = Integer.valueOf(parts[1]);
		    	
		    	firstLevel.remove(to);
		    	
		    	// Update dependency graph
		    	dependencyList.get(from).add(to);
		    	
		    }
		    
			System.out.println("Number of dependencies: " + count);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		System.out.println("Finish loading dependency file.");
		
	}
	
	public void createBatchesSlow(int numOfEdges) {
		
		int level = 0;
		
		batches = new ArrayList<Set<Integer>>();

		// Initialize level for each edge
		List<Integer> levels = new ArrayList<Integer>();
		for (int i = 0; i < numOfEdges; i++) {
			levels.add(level);
		}
		
		System.out.println();
		long start = System.currentTimeMillis();

		System.out.println("***********+BFS modified");
		
		Set<Integer> current = new HashSet<Integer>();
		current.addAll(firstLevel);
		
		Set<Integer> next = new HashSet<Integer>();
		
		
		do {
			
			System.out.println("Level " + level + ": " + current.size());
			
			level++;
			next.clear();
			
			for (Integer e: current) {
				
				if (dependencyList.get(e).size() == 0) {
					continue;
				}
				
				for (Integer edge: dependencyList.get(e)) {
					levels.set(edge, level);
					
					next.add(edge);
				}
				
			}
			
			current.clear();
			current.addAll(next);
			
		} while (current.size() > 0);
		
		
		for (int i = 0; i <= level; i++) {
			Set<Integer> set = new HashSet<Integer>();
			batches.add(set);
		}
		
		for (int i = 0; i < numOfEdges; i++) {
			int l = levels.get(i);
			batches.get(l).add(i);
		}
		
		
		int total = 0;
		for (Set<Integer> batch: batches) {
			total += batch.size();
		}
		System.out.println("Total number of edges: " + total);

		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		
		System.out.println("***********-BFS modified");

		
	}
	
	/**
	 * When a vertex appears in multiple levels,
	 * choose the deepest one
	 */
	public void createBatches() {
		System.out.println();
		long start = System.currentTimeMillis();

		System.out.println("***********+BFS modified");
		batches = new ArrayList<Set<Integer>>();
		
		Set<Integer> discovered = new HashSet<Integer>();
		
		Set<Integer> firstLevel = new HashSet<Integer>();
		for (int i = 0; i < edgesList.size(); i++) {
			if (!reverseDAG.containsKey(i)) {
				firstLevel.add(i);
			}
		}
		
		
		Set<Integer> current = new HashSet<Integer>();
		current.addAll(firstLevel);
		
		Set<Integer> next = new HashSet<Integer>();
		
		do {
			
			System.out.println("Level " + batches.size() + ": " + current.size());
			batches.add(current);
			discovered.addAll(current);

			next.clear();
					
			for (Integer e: current) {
				
				if (!dependencyDAG.containsKey(e)) {
					continue;
				}
				
				for (Integer edge: dependencyDAG.get(e)) {
					
					// Add the edge only if all its parents have been discovered
					boolean allDiscovered = true;
					for (Integer parent: reverseDAG.get(edge)) {
						if (!discovered.contains(parent)) {
							allDiscovered = false;
							break;
						}
					}
					
					if (allDiscovered && !discovered.contains(edge)) {
						next.add(edge);
					}
					
				}
			}
			
			current = new HashSet<Integer>();
			current.addAll(next);
			
		} while (current.size() > 0);
		
		int total = 0;
		for (Set<Integer> batch: batches) {
			total += batch.size();
		}
		System.out.println("Total number of edges: " + total);

		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		
		System.out.println("***********-BFS modified");

	}
	
	
	
	
	public void createBatchesSimplifiedWithList() {
		createBatchesSimplifiedWithList(-1);
	}
	
	
	
	public void createBatchesSimplifiedWithList(int numOfEdges) {
		System.out.println();
		long start = System.currentTimeMillis();

		System.out.println("***********+BFS modified");
		batches = new ArrayList<Set<Integer>>();
		
		Set<Integer> firstLevel = new HashSet<Integer>();
		
		
		if (numOfEdges == -1) {
			for (int i = 0; i < edgesList.size(); i++) {
				if (reverseList.get(i).size() == 0) {
					firstLevel.add(i);
				}
			}
		} else {
			for (int i = 0; i < numOfEdges; i++) {
				if (reverseList.get(i).size() == 0) {
					firstLevel.add(i);
				}
			}
		}
		
		
		Set<Integer> current = new HashSet<Integer>();
		current.addAll(firstLevel);
		
		Set<Integer> next = new HashSet<Integer>();
		
		do {
			
			System.out.println("Level " + batches.size() + ": " + current.size());
			batches.add(current);

			next.clear();
					
			for (Integer e: current) {
				
				if (dependencyList.get(e).size() == 0) {
					continue;
				}
				
				for (Integer edge: dependencyList.get(e)) {
					
					Set<Integer> edges = reverseList.get(edge);
					edges.remove(e);

					if (edges.size() == 0) {
						next.add(edge);
					}
					
				}
			}
			
			current = new HashSet<Integer>();
			current.addAll(next);
			
		} while (current.size() > 0);
		
		int total = 0;
		for (Set<Integer> batch: batches) {
			total += batch.size();
		}
		System.out.println("Total number of edges: " + total);

		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		
		System.out.println("***********-BFS modified");

	}

	
	public void createBatchesSimplified() {
		System.out.println();
		long start = System.currentTimeMillis();

		System.out.println("***********+BFS modified");
		batches = new ArrayList<Set<Integer>>();
		
		Set<Integer> discovered = new HashSet<Integer>();
		
		Set<Integer> firstLevel = new HashSet<Integer>();
		for (int i = 0; i < edgesList.size(); i++) {
			if (!reverseDAG.containsKey(i)) {
				firstLevel.add(i);
			}
		}
		
		
		Set<Integer> current = new HashSet<Integer>();
		current.addAll(firstLevel);
		
		Set<Integer> next = new HashSet<Integer>();
		
		do {
			
			System.out.println("Level " + batches.size() + ": " + current.size());
			batches.add(current);
			discovered.addAll(current);

			next.clear();
					
			for (Integer e: current) {
				
				if (!dependencyDAG.containsKey(e)) {
					continue;
				}
				
				for (Integer edge: dependencyDAG.get(e)) {
					
					Set<Integer> edges = reverseDAG.get(edge);
					edges.remove(e);
					
					if (edges.size() == 0) {
						next.add(edge);

					}
					
				}
			}
			
			current = new HashSet<Integer>();
			current.addAll(next);
			
		} while (current.size() > 0);
		
		int total = 0;
		for (Set<Integer> batch: batches) {
			total += batch.size();
		}
		System.out.println("Total number of edges: " + total);

		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		
		System.out.println("***********-BFS modified");

	}

	
	
	
	public void writeBatchesToFileOnlyIndex(String fileName) {
		
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + ".txt"), "utf-8"));
			
			for (Set<Integer> batch: batches) {
				for (Integer e: batch) {
					// Shouldn't write integer
					writer.write("" + e);
					writer.newLine();
				}
			}
			
			writer.close();
			
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + ".meta"), "utf-8"));
			
			for (Set<Integer> batch: batches) {
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
		
		System.out.println("Finish writing to file...");
		
	}
	
	
	public void writeBatchesToFile(String fileName) {
		
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + ".txt"), "utf-8"));
			
			for (Set<Integer> batch: batches) {
				for (Integer e: batch) {
					
					Edge edge = edgesList.get(e);
					
					String line = edge.getFrom().getId() + " " + edge.getTo().getId() + " " + edge.getDeparture() + " " + edge.getArrival();
					
					writer.write(line);
					writer.newLine();
				}
			}
			
			writer.close();
			
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + ".meta"), "utf-8"));
			
			for (Set<Integer> batch: batches) {
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
		
		System.out.println("Finish writing to file...");
		
	}
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		CreateBatchUsingDep analyzer = new CreateBatchUsingDep();

		// Social Network
		String fileName = "out.munmun_digg_reply";
//		String fileName = "out.digg-friends";
//		String fileName = "berlin";
//		String fileName = "out.wikipedia-growth";
		
		
//		analyzer.loadEdges(fileName);

//		analyzer.loadDependency(fileName);
//		analyzer.createBatchesSimplified();
		

		analyzer.loadDependencyHalf(fileName, 86203);
		analyzer.createBatchesSlow(86203);

		
//		analyzer.loadDependencyUsingList(fileName, 39953145);
//		analyzer.createBatchesSimplifiedWithList(39953145);
		
		
		
		
		analyzer.writeBatchesToFileOnlyIndex(fileName + ".index");
		
//		analyzer.writeBatchesToFile(fileName);
		
		
		
		
		
		long end = System.currentTimeMillis();
		
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}
