package sg.edu.ntu.rex.temporal.graphs.preprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import sg.edu.ntu.rex.temporal.graphs.entity.Edge;
import sg.edu.ntu.rex.temporal.graphs.entity.Vertex;

public class ReplaceIndexWithEdges {
	
	private static final boolean DEBUG = true;
	
	private List<Edge> edges;
	
	
	public ReplaceIndexWithEdges() {
	}

	
	public void loadEdges(String file) {
		
		edges = new ArrayList<Edge>();
		
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
		    		    	
		    	parts = line.split(" ");
		    	
		    	from = new Vertex(parts[1]);
		    	to = new Vertex(parts[2]);
		    	departure = Long.valueOf(parts[3]);
		    	arrival = Long.valueOf(parts[4]);
		    	
		    	edge = new Edge(from, to, departure, arrival);
		    	
		    	
				// Add edge
		    	edges.add(edge);
		    	
		    }
		    
			System.out.println("Number of edges: " + edges.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish loading edge file.");
		
	}
	
	
	public void replaceIndex(String file) {
		
		System.out.println("Start to load index file.");
		
		try {
			
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("Output/" + file + ".edge.txt"), "utf-8"));
			
			
			BufferedReader br = new BufferedReader(new FileReader("Output/" + file + "2.txt"));
		    String line;
		    int index;
		    
		    long count = 0;
		    
		    while ((line = br.readLine()) != null) {
		    	
		    	count++;
		    	if (count % 1000000 == 0 && DEBUG) {
					System.out.println("Reading line " + count + "...");
				}
		    		    	
		    	
		    	index = Integer.valueOf(line);
		    	
		    	Edge e = edges.get(index);
		    	
		    	String content = e.getFrom().getId() + " " + e.getTo().getId() + " " + e.getDeparture() + " " + e.getArrival();
		    	writer.write(content);
				writer.newLine();
		    	
		    }
		    
			writer.close();
			br.close();
		    
			System.out.println("Number of edges: " + edges.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish loading edge file.");
		
	}
	
	
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		ReplaceIndexWithEdges analyzer = new ReplaceIndexWithEdges();

		// Social Network
//		String fileName = "out.munmun_digg_reply";
//		String fileName = "out.digg-friends";
//		String fileName = "berlin";
		String fileName = "out.wikipedia-growth";
		
		
		analyzer.loadEdges(fileName);
		analyzer.replaceIndex(fileName);
		
		
		long end = System.currentTimeMillis();
		
		
		System.out.println("Total processing time: " + (end-start)*1.0/1000 + " seconds.");
	}

}

