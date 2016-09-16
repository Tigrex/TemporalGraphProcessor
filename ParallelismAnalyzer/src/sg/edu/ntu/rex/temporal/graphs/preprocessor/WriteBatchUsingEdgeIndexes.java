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
import java.util.List;
import java.util.Set;

public class WriteBatchUsingEdgeIndexes {
	
	private static final boolean DEBUG = true;
	
	private List<List<Integer>> reverseList;
	
	private List<Integer> ts;
	
	private List<Set<Integer>> batches;

	
	public WriteBatchUsingEdgeIndexes() {
	}

	
	public void loadTS(String file) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load ts file.");
		
		
		ts = new ArrayList<Integer>();
		
		try (BufferedReader br = new BufferedReader(new FileReader("Temp/" + file + ".ts"))) {
		    String line;
		    
		    int element;
		    
		    long count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	
		    	element = Integer.valueOf(line);
		    	
		    	ts.add(element);
		    	
		    }
		    
			System.out.println("Number of edges: " + count);
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Total time: " + (end-start)*1.0/1000 + " seconds.");
		System.out.println("Finish loading ts file.");
		
	}
	
	
	
	public void loadReverseList(String file, int numOfEdges) {
		
		long start = System.currentTimeMillis();

		System.out.println("Start to load dependency file.");
		
		
		reverseList = new ArrayList<List<Integer>>();
		for (int i = 0; i < numOfEdges; i++) {
			reverseList.add(new ArrayList<Integer>());
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
		    	reverseList.get(to).add(from);
		    	
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
	
	
	public void markLevel(int numOfEdges) {
		long start = System.currentTimeMillis();

		int maxLevel = 0;
		// Initialize level for each edge
		List<Integer> levels = new ArrayList<Integer>();
		for (int i = 0; i < numOfEdges; i++) {
			levels.add(0);
		}
	
		
		for (int element: ts) {
			
			List<Integer> list = reverseList.get(element);
			
			if (list.size() == 0) {
				continue;
			}
			
			int max = 0;
			for (int i: list) {
				
				int level = levels.get(i);
				if (level > max) {
					max = level;
				}
				
			}
			
			levels.set(element, max + 1);
			
			if (maxLevel < max + 1) {
				maxLevel = max + 1;
			}
			
		}
		
		batches = new ArrayList<Set<Integer>>();
		
		for (int i = 0; i <= maxLevel; i++) {
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
		
		
	}
	
	public void writeBatchesToFileOnlyIndex(String fileName) {
		
		System.out.println("Start to write to file...");
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + "2.txt"), "utf-8"));
			
			for (Set<Integer> batch: batches) {
				for (Integer e: batch) {
					// Shouldn't write integer
					writer.write("" + e);
					writer.newLine();
				}
			}
			
			writer.close();
			
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + fileName + "2.meta"), "utf-8"));
			
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
		WriteBatchUsingEdgeIndexes analyzer = new WriteBatchUsingEdgeIndexes();

		// Social Network
//		String fileName = "out.munmun_digg_reply";
//		String fileName = "out.digg-friends";
//		String fileName = "berlin";
		String fileName = "out.wikipedia-growth";
		
		
		
		analyzer.loadTS(fileName);
		analyzer.loadReverseList(fileName, 39953145);
		
		analyzer.markLevel(39953145);
		
		analyzer.writeBatchesToFileOnlyIndex(fileName);
		
		
		long end = System.currentTimeMillis();
		
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}


