package sg.edu.ntu.rex.temporal.graphs.analyzer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.serialization.GtfsReader;

public class GtfsLoader {
	
	private Map<Trip, List<StopTime>> trips;
	private GtfsDaoImpl store;
	private boolean log = false;
	
	public GtfsLoader(String path) throws IOException {
		loadDAO(path);
		ServiceDate busiestDay = getBusiestDay();
		Set<AgencyAndId> services = getAllServicesForDay(busiestDay);
		Set<Trip> allTrips = getAllTripsForServices(services);
		trips = getAllStopTimesForTrips(allTrips);
	}
	
	
	public Map<Trip, List<StopTime>> getTrips() {
		return trips;
	}

	private void loadDAO(String path) throws IOException {
		File folder = new File(path);
		GtfsReader reader = new GtfsReader();
		reader.setInputLocation(folder);
		store = new GtfsDaoImpl();
		reader.setEntityStore(store);
		reader.run();
		
		if (log) {
			System.out.println("The total number of stops: " + store.getAllStops().size());
			System.out.println("The total number of stop times: " + store.getAllStopTimes().size());
			System.out.println("The total number of routes: " + store.getAllRoutes().size());
			System.out.println("The total number of trips: " + store.getAllTrips().size());
			System.out.println("The total number of transfers: " + store.getAllTransfers().size());
			System.out.println("The total number of calendars: " + store.getAllCalendars().size());
			System.out.println("The total number of calendar dates: " + store.getAllCalendarDates().size());
			System.out.println("The total number of frequencies: " + store.getAllFrequencies().size());
			System.out.println("The total number of pathways: " + store.getAllPathways().size());
		}
		
	}
	
	private ServiceDate getBusiestDay() {
		Map<ServiceDate, Integer> dateCountMap = new HashMap<ServiceDate, Integer>();
		for (ServiceCalendarDate cdate: store.getAllCalendarDates()) {
			ServiceDate sDate = cdate.getDate();
			if (!dateCountMap.containsKey(sDate)) {
				dateCountMap.put(sDate, 1);
			} else {
				int oldValue = dateCountMap.remove(sDate);
				dateCountMap.put(sDate, oldValue + 1);
			}
		}
		
		ServiceDate busiestDay = null;
		int numOfServices = -1;
		for (ServiceDate sDate:dateCountMap.keySet()) {
			if (dateCountMap.get(sDate) > numOfServices) {
				numOfServices = dateCountMap.get(sDate);
				busiestDay = sDate;
			}
		}
		
		if (log) {
			System.out.println("The total number of calendar days: " + dateCountMap.size()); 
			System.out.println("The busiest day " + busiestDay + ": " + numOfServices); 
		}
		
		return busiestDay;
	}
	
	private Set<AgencyAndId> getAllServicesForDay(ServiceDate day) {
		Set<AgencyAndId> services = new HashSet<AgencyAndId>();
		for (ServiceCalendarDate cdate: store.getAllCalendarDates()) {
			if (cdate.getDate().equals(day)) {
				services.add(cdate.getServiceId());
			}
		}
		
		if (log) {
			System.out.println("The total number of services on " + day + ": " + services.size());
		}
		
		return services;
	}
	
	private Set<Trip> getAllTripsForServices(Set<AgencyAndId> services) {
		Set<Trip> trips = new HashSet<Trip>();
		for (Trip trip: store.getAllTrips()) {
			if (services.contains(trip.getServiceId())) {
				trips.add(trip);
			}
		}
		
		if (log) {
			System.out.println("The total number of trips: " + trips.size());
		}
		
		return trips;
	}
	
	private Map<Trip, List<StopTime>> getAllStopTimesForTrips(Set<Trip> trips) {
		Map<Trip, List<StopTime>> stopTimesMap = new HashMap<Trip, List<StopTime>>();
		for (StopTime stopTime: store.getAllStopTimes()) {
			Trip trip = stopTime.getTrip();
			if (trips.contains(trip)) {
				if (stopTimesMap.containsKey(trip)) {
					List<StopTime> times = stopTimesMap.get(trip);
					times.add(stopTime);
				} else {
					List<StopTime> times = new ArrayList<StopTime>();
					times.add(stopTime);
					stopTimesMap.put(trip, times);
				}
			}
		}
		
		for (List<StopTime> stopTimes: stopTimesMap.values()) {
			Collections.sort(stopTimes);
		}
		return stopTimesMap;
	}
	
	public static void main(String[] args) throws IOException {
		GtfsLoader gtfsLoader = new GtfsLoader("GTFS/Berlin");
		Map<Trip, List<StopTime>> stopTimes = gtfsLoader.getTrips();
		
		for (Trip trip: stopTimes.keySet()) {
			System.out.println("Trip " + trip + ": " + stopTimes.get(trip).size());
		}
		
	}
	
}