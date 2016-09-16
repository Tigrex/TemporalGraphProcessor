package sg.edu.ntu.rex.temporal.graphs.entity;

public class Vertex implements Comparable<Vertex> {
	
	private String id;
	
	private int degree;

	public Vertex(String id) {
		this.id = id;
		this.degree = 0;
	}
	
	public String getId() {
		return id;
	}

	public int getDegree() {
		return degree;
	}
	
	public void setDegree(int degree) {
		this.degree = degree;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		Vertex other = (Vertex) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Vertex [id=" + id + "]";
	}

	@Override
	public int compareTo(Vertex o) {
		return Integer.compare(this.getDegree(), o.getDegree());
	}

}
