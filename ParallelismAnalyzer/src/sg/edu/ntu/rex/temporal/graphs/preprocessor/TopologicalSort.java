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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class TopologicalSort {
	
	private static final boolean DEBUG = true;
	
	private List<List<Integer>> dependencyList;
	
	private Set<Integer> firstLevel;
	
	List<Set<Integer>> batches;
	
	
	public TopologicalSort() {
	}

	
	public void loadDependencyUsingList(String file, int numOfEdges) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load dependency file.");
		
		firstLevel = new HashSet<Integer>();
		for (int i = 0; i < numOfEdges; i++) {
			firstLevel.add(i);
		}
		
		
		dependencyList = new ArrayList<List<Integer>>();
		for (int i = 0; i < numOfEdges; i++) {
			dependencyList.add(new ArrayList<Integer>());
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
		    	
		    	// Update dependency graph
		    	dependencyList.get(from).add(to);
		    	
		    	firstLevel.remove(to);
		    	
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
	
	
	public void topologicalSort(String fileName) {
		
		List<Integer> ts = new LinkedList<Integer>();
		
		Stack<Integer> stack = new Stack<Integer>();
		stack.addAll(firstLevel);
		
		Set<Integer> visited = new HashSet<Integer>();
		
		while(stack.size() > 0) {
			int element = stack.peek();
			
			List<Integer> deps = dependencyList.get(element);
			
			if (deps.size() == 0) {
				stack.pop();
				
				if (!visited.contains(element)) {
					visited.add(element);
					ts.add(0, element);
				}
				
			} else {
				int next = deps.remove(0);
				stack.push(next);
			}
			
		}
		
		System.out.println("Number of edges in ts: " + ts.size());
		
		
		System.out.println("Start to write ts to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Temp/" + fileName + ".ts"), "utf-8"));
			
			for (Integer i: ts) {
				writer.write("" + i);
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
		
		System.out.println("Finish writing ts to file...");
		
	}
	
	
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		TopologicalSort analyzer = new TopologicalSort();

		// Social Network
//		String fileName = "out.munmun_digg_reply";
//		String fileName = "out.digg-friends";
//		String fileName = "berlin";
		String fileName = "out.wikipedia-growth";
		
		
		
		analyzer.loadDependencyUsingList(fileName, 39953145);

		analyzer.topologicalSort(fileName);
		
		
		long end = System.currentTimeMillis();
		
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}

