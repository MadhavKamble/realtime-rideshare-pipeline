CITY_ZONES = {
    "airport": {
        "lat_center": 13.1986,
        "lon_center": 77.7066,
        "lat_std": 0.005,
        "lon_std": 0.005,
        "demand_multiplier": 1.8,
        "cancellation_rate": 0.04,
    },
    "railway_station": {
        "lat_center": 12.9784,
        "lon_center": 77.5707,
        "lat_std": 0.008,
        "lon_std": 0.008,
        "demand_multiplier": 2.2,
        "cancellation_rate": 0.10,
    },
    "cbd": {
        "lat_center": 12.9716,
        "lon_center": 77.5946,
        "lat_std": 0.015,
        "lon_std": 0.015,
        "demand_multiplier": 1.5,
        "cancellation_rate": 0.07,
    },
    "mall": {
        "lat_center": 12.9352,
        "lon_center": 77.6245,
        "lat_std": 0.010,
        "lon_std": 0.010,
        "demand_multiplier": 1.2,
        "cancellation_rate": 0.06,
    },
    "residential": {
        "lat_center": 13.0200,
        "lon_center": 77.5800,
        "lat_std": 0.030,
        "lon_std": 0.030,
        "demand_multiplier": 1.0,
        "cancellation_rate": 0.08,
    },
}

VEHICLE_TYPES = ["bike", "auto", "cab_economy", "cab_premium"]
VEHICLE_BASE_FARE = {"bike": 15, "auto": 25, "cab_economy": 40, "cab_premium": 80}
VEHICLE_WEIGHTS = [0.30, 0.25, 0.35, 0.10]

PEAK_HOURS_MORNING = range(8, 11)
PEAK_HOURS_EVENING = range(17, 21)
PEAK_DEMAND_MULTIPLIER = 3.0
OFF_PEAK_DEMAND_MULTIPLIER = 0.3

LATE_EVENT_PROBABILITY = 0.05
LATE_EVENT_DELAY_MS_MIN = 30_000
LATE_EVENT_DELAY_MS_MAX = 120_000

WEATHER_STATES = ["clear", "cloudy", "rain"]
WEATHER_DEMAND_MULTIPLIER = {"clear": 1.0, "cloudy": 1.1, "rain": 1.6}
