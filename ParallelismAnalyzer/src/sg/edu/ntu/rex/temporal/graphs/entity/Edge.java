package sg.edu.ntu.rex.temporal.graphs.entity;
import java.util.Comparator;

public class Edge {

	private Vertex from;
	private Vertex to;
	private long departure;
	private long arrival;
	
	public Edge(Vertex from, Vertex to, long departure, long arrival) {
		this.from = from;
		this.to = to;
		this.departure = departure;
		this.arrival = arrival;
	}

	public Edge(long departure, long arrival) {
		this.departure = departure;
		this.arrival = arrival;
	}

	public Vertex getFrom() {
		return this.from;
	}

	public Vertex getTo() {
		return this.to;
	}

	public Long getDeparture() {
		return this.departure;
	}
	
	public Long getArrival() {
		return this.arrival;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (arrival ^ (arrival >>> 32));
		result = prime * result + (int) (departure ^ (departure >>> 32));
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + ((to == null) ? 0 : to.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Edge other = (Edge) obj;
		if (arrival != other.arrival)
			return false;
		if (departure != other.departure)
			return false;
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
		if (to == null) {
			if (other.to != null)
				return false;
		} else if (!to.equals(other.to))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		
		return this.from.getId() + " " + this.to.getId() + " " + this.departure + " " + this.arrival;		
		
	}
	
	public static class SorterByArrival implements Comparator<Edge> {
		@Override
		public int compare(Edge o1, Edge o2) {
			return Long.compare(o1.getArrival(), o2.getArrival());
		}
	}
	
	public static class SorterByDeparture implements Comparator<Edge> {
		@Override
		public int compare(Edge o1, Edge o2) {
			return Long.compare(o1.getDeparture(), o2.getDeparture());
		}
	}
	
	
}
