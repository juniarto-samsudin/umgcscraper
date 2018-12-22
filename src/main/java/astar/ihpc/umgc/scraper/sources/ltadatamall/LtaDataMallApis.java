package astar.ihpc.umgc.scraper.sources.ltadatamall;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Just a listing of all the LTA DataMall published APs
 * Updated for Version 4.8 (21 Sep 2018) of the API User Guide & Documentation.
 * See {@link https://www.mytransport.sg/content/dam/datamall/datasets/LTA_DataMall_API_User_Guide.pdf} for the user guide.
 * 
 * @author othmannb
 *
 */
public class LtaDataMallApis {
	/**
	 * Returns real-time Bus Arrival information of Bus Services at a queried Bus Stop,
	 * including Est. Arrival Time, Est. Current Location, Est. Current Load.
	 * <p>
	 * Update freq: 1 minute
	 * <br>
	 * Parameters: BusStopCode (mandatory), ServiceNo (optional)
	 */
	public static final String BUS_ARRIVAL = "http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2";

	/**
	 * Returns detailed service information for all buses currently in
	 * operation, including: first stop, last stop, peak / offpeak frequency of
	 * dispatch.
	 * <p>
	 * Update freq: Ad-hoc
	 */
	public static final String BUS_SERVICES = "http://datamall2.mytransport.sg/ltaodataservice/BusServices";
	
	/**
	 * Returns detailed route information for all services currently in operation,
	 * including: all bus stops along each route, first/last bus timings for each stop.
	 * <p>
	 * Update freq: Ad-hoc
	 */
	public static final String BUS_ROUTES = "http://datamall2.mytransport.sg/ltaodataservice/BusRoutes";

	/**
	 * Returns detailed information for all bus stops currently being serviced by
	 * buses, including: Bus Stop Code, location coordinates.
	 * <p>
	 * Update freq: Ad-hoc
	 */
	public static final String BUS_STOPS = "http://datamall2.mytransport.sg/ltaodataservice/BusStops";

	/**
	 * Returns tap in and tap out passenger volume by weekdays and
	 * weekends for individual bus stop
	 * <p>
	 * Update freq: By 15th of every month, the passenger volume for previous month data
	 * will be generated
	 * <br>
	 * Parameters: Date (optional, in YYYYMM format)
	 */
	public static final String PASSENGER_VOLUME_BY_BUS_STOPS = "http://datamall2.mytransport.sg/ltaodataservice/PV/Bus";

	/**
	 * Returns number of trips by weekdays and weekends from origin to
	 * destination bus stops
	 * <p>
	 * Update freq: By 15th of every month, the passenger volume for previous month data
	 * will be generated
	 * <br>
	 * Parameters: Date (optional, in YYYYMM format)
	 */
	public static final String PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_BUS_STOPS = "http://datamall2.mytransport.sg/ltaodataservice/PV/ODBus";

	/**
	 * Returns number of trips by weekdays and weekends from origin to
	 * destination train stations
	 * <p>
	 * Update freq: By 15th of every month, the passenger volume for previous month data
	 * will be generated
	 * <br>
	 * Parameters: Date (optional, in YYYYMM format)
	 */
	public static final String PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_TRAIN_STATIONS = "http://datamall2.mytransport.sg/ltaodataservice/PV/ODTrain";

	/**
	 * Returns tap in and tap out passenger volume by weekdays and
	 * weekends for individual train station
	 * <p>
	 * Update freq: By 15th of every month, the passenger volume for previous month data
	 * will be generated
	 * <br>
	 * Parameters: Date (optional, in YYYYMM format)
	 */
	public static final String PASSENGER_VOLUME_BY_TRAIN_STATIONS = "http://datamall2.mytransport.sg/ltaodataservice/PV/Train";
	
	/**
	 * Returns location coordinates of all Taxis that are currently available for
	 * hire. Does not include "Hired" or "Busy" Taxis.
	 * <p>
	 * Update freq: 1 min
	 */
	public static final String TAXI_AVAILABILITY = "http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability";

	/**
	 * Returns detailed information on train service unavailability during scheduled
	 * operating hours, such as affected line and stations etc
	 * <p>
	 * Update freq: Ad-hoc
	 */
	public static final String TRAIN_SERVICE_ALERTS = "http://datamall2.mytransport.sg/ltaodataservice/TrainServiceAlerts";

	/**
	 * Returns no. of available lots for HDB, LTA and URA carpark data.
	 * The LTA carpark data consist of major shopping malls and developments within
	 * Orchard, Marina, HarbourFront, Jurong Lake District.
	 * (Note: list of LTA carpark data available on this API is subset of those listed on
	 * One.Motoring and MyTransport Portals)
	 * <p>
	 * Update freq: 1 min
	 */
	public static final String CARPARK_AVAILABILITY = "http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2";

	/**
	 * Returns ERP rates of all vehicle types across all timings for each
	 * zone.
	 * <p>
	 * Update freq: Ad-hoc
	 */
	public static final String ERP_RATES = "http://datamall2.mytransport.sg/ltaodataservice/ERPRates";

	/**
	 * Returns estimated travel times of expressways (in segments).
	 * <p>
	 * Update freq: 5 min
	 */
	public static final String ESTIMATED_TRAVEL_TIMES = "http://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes";

	/**
	 * Returns alerts of traffic lights that are currently faulty, or currently
	 * undergoing scheduled maintenance.
	 * <p>
	 * Update freq: 2 min (whenever there are updates)
	 */
	public static final String FAULTY_TRAFFIC_LIGHTS = "http://datamall2.mytransport.sg/ltaodataservice/FaultyTrafficLights";

	/**
	 * Returns all planned road openings.
	 * <p>
	 * Update freq: 24 hrs (whenever there are updates)
	 */
	public static final String ROAD_OPENINGS = "http://datamall2.mytransport.sg/ltaodataservice/RoadOpenings";

	/**
	 * Returns all road works being / to be carried out.
	 * <p>
	 * Update freq: 24 hrs (whenever there are updates)
	 */
	public static final String ROAD_WORKS = "http://datamall2.mytransport.sg/ltaodataservice/RoadWorks";

	/**
	 * Returns links to images of live traffic conditions along expressways and Woodlands & Tuas Checkpoints.
	 * <p>
	 * Update freq: 1 to 5 min
	 */
	public static final String TRAFFIC_IMAGES = "http://datamall2.mytransport.sg/ltaodataservice/Traffic-Images";

	/**
	 * Returns incidents currently happening on the roads, such as
	 * Accidents, Vehicle Breakdowns, Road Blocks, Traffic Diversions etc.
	 * <p>
	 * Update freq: 2 min (whenever there are updates)
	 */
	public static final String TRAFFIC_INCIDENTS = "http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents";

	/**
	 * Returns current traffic speeds on expressways and arterial roads,
	 * expressed in speed bands.
	 * <p>
	 * Update freq: 5 min
	 */
	public static final String TRAFFIC_SPEED_BANDS = "http://datamall2.mytransport.sg/ltaodataservice/TrafficSpeedBandsv2";

	/**
	 * Returns traffic advisories (via variable message services) concerning
	 * current traffic conditions that are displayed on EMAS signboards
	 * along expressways and arterial roads.
	 * <p>
	 * Update freq: 2 min
	 */
	public static final String VMS_EMAS = "http://datamall2.mytransport.sg/ltaodataservice/VMS";
	
	public static final Map<String, String> API_MAP;
	public static final Set<String> API_NAMES;
	public static final Set<String> REAL_TIME_API_NAMES;
	public static final Set<String> REAL_TIME_PARAMETRIC_API_NAMES;
	public static final Set<String> REAL_TIME_NON_PARAMETRIC_API_NAMES;
	public static final Set<String> ADHOC_API_NAMES;
	public static final Set<String> ADHOC_FREQUENT_API_NAMES;
	public static final Set<String> ADHOC_INFREQUENT_API_NAMES;
	public static final Set<String> MONTHLY_API_NAMES;
	
	static {
		
		Map<String, String> m = new LinkedHashMap<>();
		m.put("BUS_ARRIVAL", BUS_ARRIVAL);
		m.put("BUS_SERVICES", BUS_SERVICES);
		m.put("BUS_ROUTES", BUS_ROUTES);
		m.put("BUS_STOPS", BUS_STOPS);
		m.put("PASSENGER_VOLUME_BY_BUS_STOPS", PASSENGER_VOLUME_BY_BUS_STOPS);
		m.put("PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_BUS_STOPS", PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_BUS_STOPS);
		m.put("PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_TRAIN_STATIONS", PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_TRAIN_STATIONS);
		m.put("PASSENGER_VOLUME_BY_TRAIN_STATIONS", PASSENGER_VOLUME_BY_TRAIN_STATIONS);
		m.put("TAXI_AVAILABILITY", TAXI_AVAILABILITY);
		m.put("TRAIN_SERVICE_ALERTS", TRAIN_SERVICE_ALERTS);
		m.put("CARPARK_AVAILABILITY", CARPARK_AVAILABILITY);
		m.put("ERP_RATES", ERP_RATES);
		m.put("ESTIMATED_TRAVEL_TIMES", ESTIMATED_TRAVEL_TIMES);
		m.put("FAULTY_TRAFFIC_LIGHTS", FAULTY_TRAFFIC_LIGHTS);
		m.put("ROAD_OPENINGS", ROAD_OPENINGS);
		m.put("ROAD_WORKS", ROAD_WORKS);
		m.put("TRAFFIC_IMAGES", TRAFFIC_IMAGES);
		m.put("TRAFFIC_INCIDENTS", TRAFFIC_INCIDENTS);
		m.put("TRAFFIC_SPEED_BANDS", TRAFFIC_SPEED_BANDS);
		m.put("VMS_EMAS", VMS_EMAS);
		
		API_MAP = Collections.unmodifiableMap(m);
		API_NAMES = m.keySet();
		
		REAL_TIME_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"BUS_ARRIVAL", "TAXI_AVAILABILITY", "TRAIN_SERVICE_ALERTS", "CARPARK_AVAILABILITY", "ESTIMATED_TRAVEL_TIMES", "FAULTY_TRAFFIC_LIGHTS", "ROAD_OPENINGS", "ROAD_WORKS",
				"TRAFFIC_IMAGES", "TRAFFIC_INCIDENTS", "TRAFFIC_SPEED_BANDS", "VMS_EMAS"
		)));
		
		REAL_TIME_NON_PARAMETRIC_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"TAXI_AVAILABILITY", "TRAIN_SERVICE_ALERTS", "CARPARK_AVAILABILITY", "ESTIMATED_TRAVEL_TIMES", "FAULTY_TRAFFIC_LIGHTS", "ROAD_OPENINGS", "ROAD_WORKS",
				"TRAFFIC_IMAGES", "TRAFFIC_INCIDENTS", "TRAFFIC_SPEED_BANDS", "VMS_EMAS"
		)));
		
		REAL_TIME_PARAMETRIC_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"BUS_ARRIVAL"
		)));
		
		ADHOC_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"BUS_SERVICES", "BUS_ROUTES", "BUS_STOPS", "TRAIN_SERVICE_ALERTS", "ERP_RATES"
		)));
		
		ADHOC_FREQUENT_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"TRAIN_SERVICE_ALERTS"
		)));
		
		ADHOC_INFREQUENT_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"BUS_SERVICES", "BUS_ROUTES", "BUS_STOPS", "ERP_RATES"
		)));
		
		MONTHLY_API_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
				"PASSENGER_VOLUME_BY_BUS_STOPS", "PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_BUS_STOPS", "PASSENGER_VOLUME_BY_ORIGIN_DESTINATION_TRAIN_STATIONS", "PASSENGER_VOLUME_BY_TRAIN_STATIONS"
		)));
				
	}
}
