from simulator.driver_generator import generate_driver_event
from simulator.payment_generator import generate_payment_event
from simulator.ride_generator import compute_surge_multiplier, generate_ride_event


def test_generate_ride_event_schema():
    event = generate_ride_event()
    assert event["schema_version"] == "1.0"
    assert event["ride_id"]
    assert 1.0 <= event["surge_multiplier"] <= 3.5


def test_generate_driver_event_schema():
    event = generate_driver_event()
    assert event["schema_version"] == "1.0"
    assert event["driver_id"]


def test_generate_payment_event_schema():
    event = generate_payment_event()
    assert event["schema_version"] == "1.0"
    assert event["payment_id"]


def test_surge_multiplier_bounds():
    surge = compute_surge_multiplier("cbd", 9, "clear")
    assert 1.0 <= surge <= 3.5
