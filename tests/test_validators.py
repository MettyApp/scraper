import pytest

from validators import validate


def base_record(**overrides):
    data = {
        "origin_countries": [],
        "origin_regions": [],
        "origin_farms": [],
        "coffee_producers": [],
        "varieties": [],
        "processes": [],
        "price_per_kilo": None,
    }
    data.update(overrides)
    return data


def test_fill_from_producer_populates_all_levels():
    # Colombia/Huila -> Las Flores -> Jhoan Vergara
    record = base_record(coffee_producers=["Jhoan Vergara"])  # only producer provided
    out = validate(record)

    assert set(out["coffee_producers"]) == {"Jhoan Vergara"}
    assert "Las Flores" in out["origin_farms"]
    assert "huila" in out["origin_regions"]
    assert "COLOMBIA" in out["origin_countries"]


def test_fill_from_farm_populates_region_and_country():
    # Colombia/Huila -> Finca La Filadelfia (producer exists but farm validator doesn't return it)
    record = base_record(origin_farms=["Finca La Filadelfia"])  # only farm provided
    out = validate(record)

    assert "Finca La Filadelfia" in out["origin_farms"]
    assert "huila" in out["origin_regions"]
    assert "COLOMBIA" in out["origin_countries"]


def test_fill_from_region_populates_country():
    record = base_record(origin_regions=["huila"])  # only region provided
    out = validate(record)

    assert "huila" in out["origin_regions"]
    assert "COLOMBIA" in out["origin_countries"]


def test_contradiction_resolution_producer_overrides_all():
    # Intentionally conflicting inputs. Producer should win over farm/region/country
    record = base_record(
        coffee_producers=["Jhoan Vergara"],
        origin_farms=["Some Wrong Farm"],
        origin_regions=["cauca"],
        origin_countries=["PERU"],
    )
    out = validate(record)

    # Producer-derived truth
    assert set(out["coffee_producers"]) == {"Jhoan Vergara"}
    assert "Las Flores" in out["origin_farms"]
    assert set(out["origin_regions"]) == {"huila"}
    assert set(out["origin_countries"]) == {"COLOMBIA"}


def test_contradiction_resolution_farm_overrides_region_and_country():
    # Panama/Chiriqui -> Elida Estate, but contradicting region provided
    record = base_record(
        origin_farms=["Elida Estate"],
        origin_regions=["veraguas"],
        origin_countries=["ECUADOR"],
    )
    out = validate(record)

    assert "Elida Estate" in out["origin_farms"]
    assert set(out["origin_regions"]) == {"chiriqui"}
    assert set(out["origin_countries"]) == {"PANAMA"}


def test_contradiction_resolution_region_overrides_country():
    record = base_record(
        origin_regions=["huila"],
        origin_countries=["PERU"],
    )
    out = validate(record)

    assert set(out["origin_regions"]) == {"huila"}
    assert set(out["origin_countries"]) == {"COLOMBIA"}