package sg.edu.ntu.rex.temporal.graphs.generator;

public class GraphGenerator {

	public static void main(String[] args) {
		
		int maxTime = 500;
		int numOfInstances = 1;
		
		int numOfVertices = 10000;
		
		boolean generateBatch = true;
		
		
		StaticGraphGenerator generator = new StaticGraphGenerator();

		// Erdos-Renyi
		StaticGraph staticGraph = generator.erdosRenyi(numOfVertices, 0.002);
		System.out.println("Static graph size: " + staticGraph.getNumOfEdges());
		Graph temporalGraph = staticGraph.toTemporalGraph(maxTime, numOfInstances);
		System.out.println("Temporal graph size: " + temporalGraph.getNumOfEdges());
		if (generateBatch) {
			temporalGraph.createBatches();
		}
		System.out.println();
		
		
		// Watts-Strogatz
//		StaticGraph staticGraph = generator.wattsStrogatz(numOfVertices, 20, 0.7);
//		System.out.println("Static graph size: " + staticGraph.getNumOfEdges());
//		Graph temporalGraph = staticGraph.toTemporalGraph(maxTime, numOfInstances);
//		System.out.println("Temporal graph size: " + temporalGraph.getNumOfEdges());
//		if (generateBatch) {
//			temporalGraph.createBatches();
//		}
//		System.out.println();
		

		// Barabasi-Albert
//		StaticGraph staticGraph = generator.barabasiAlbert(numOfVertices, 10);
//		System.out.println("Static graph size: " + staticGraph.getNumOfEdges());
//		Graph temporalGraph = staticGraph.toTemporalGraph(maxTime, numOfInstances);
//		System.out.println("Temporal graph size: " + temporalGraph.getNumOfEdges());
//		if (generateBatch) {
//			temporalGraph.createBatches();
//		}
//		System.out.println();
		
		
		// temporalGraph.writeEdgeStream("test");
		
	}

}
