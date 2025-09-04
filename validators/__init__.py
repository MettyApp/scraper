from validators.country import CountryValidator
from validators.pricePerKilo import PricePerKiloValidator
from validators.processes import ProcessesValidator
from validators.producer import ProducerValidator
from validators.region import RegionValidator
from validators.varieties import VarietiesValidator
from validators.farm import FarmValidator

country_validator = CountryValidator()
varieties_validator = VarietiesValidator()
processes_validator = ProcessesValidator()
region_validator = RegionValidator()
farm_validator = FarmValidator()
producer_validator = ProducerValidator()
price_validator = PricePerKiloValidator()


def apply_and_dedup_array(d, key, validator):
    if key in d and d[key] is not None:
        current_value = d[key]
        d[key] = list(elt for elt in 
            set(
                v
                for v in [validator(v) for v in current_value]
                if v is not None and len(v) > 0
            )
        )
    else:
        d[key] = []
    return d


def validate(d):
    if d is None:
        return {}
    apply_and_dedup_array(d, "origin_countries", country_validator.validate)
    apply_and_dedup_array(d, "origin_regions", region_validator.validate)
    apply_and_dedup_array(d, "varieties", varieties_validator.validate)
    apply_and_dedup_array(d, "processes", processes_validator.validate)
    apply_and_dedup_array(d, "origin_farms", farm_validator.validate)
    apply_and_dedup_array(d, "coffee_producers", producer_validator.validate)

    d["price_per_kilo"] = price_validator.validate(d["price_per_kilo"])

    producers = d["coffee_producers"]
    farms = d["origin_farms"]
    regions = d["origin_regions"]
    countries = d["origin_countries"]

    # Apply contradiction resolution and fill missing fields using priority:
    # producer > farm > region > country

    # 1) If producers present, derive farms/regions/countries from producers and override
    if len(producers) > 0:
        producer_countries = set([p[0] for p in producers if len(p) > 0 and p[0] is not None])
        producer_regions = set([(p[0], p[1]) for p in producers if len(p) > 1 and p[0] is not None and p[1] is not None])
        producer_farms = set([(p[0], p[1], p[2]) for p in producers if len(p) > 2 and p[0] is not None and p[1] is not None and p[2] is not None])

        if len(producer_farms) > 0:
            farms = list(producer_farms)
        # If no farm info in producers but we have farms, keep only those consistent with producer regions/countries
        elif len(farms) > 0:
            if len(producer_regions) > 0:
                farms = list(set([f for f in farms if (f[0], f[1]) in producer_regions]))
            elif len(producer_countries) > 0:
                farms = list(set([f for f in farms if f[0] in producer_countries]))

        if len(producer_regions) > 0:
            regions = list(producer_regions)
        elif len(farms) > 0:
            regions = list(set([(f[0], f[1]) for f in farms if f[0] is not None and f[1] is not None]))

        if len(producer_countries) > 0:
            countries = list(producer_countries)
        elif len(regions) > 0:
            countries = list(set([r[0] for r in regions if r[0] is not None]))

    # 2) Else if farms present, derive regions/countries from farms and override
    elif len(farms) > 0:
        farm_regions = set([(f[0], f[1]) for f in farms if f[0] is not None and f[1] is not None])
        farm_countries = set([f[0] for f in farms if f[0] is not None])

        if len(farm_regions) > 0:
            regions = list(farm_regions)
        if len(farm_countries) > 0:
            countries = list(farm_countries)

        # If regions existed, keep only farms consistent with them
        if len(d["origin_regions"]) > 0:
            regions_set = set(regions)
            farms = list(set([f for f in farms if (f[0], f[1]) in regions_set]))

    # 3) Else if regions present, derive countries from regions and override
    elif len(regions) > 0:
        region_countries = set([r[0] for r in regions if r[0] is not None])
        if len(region_countries) > 0:
            countries = list(region_countries)

        # If countries existed, keep only regions consistent with them
        if len(d["origin_countries"]) > 0:
            countries_set = set(countries)
            regions = list(set([r for r in regions if r[0] in countries_set]))

    # 4) Countries-only case: nothing to fill downward

    # Assign back the resolved lists
    d["origin_farms"] = farms
    d["origin_regions"] = regions
    d["origin_countries"] = countries

    # Fill missing lower levels when possible
    if len(d["origin_farms"]) > 0:
        if len(d["origin_regions"]) == 0:
            d["origin_regions"] = list(set([(e[0], e[1],) for e in d["origin_farms"] if e[0] is not None and e[1] is not None]))
        if len(d["origin_countries"]) == 0:
            d["origin_countries"] = list(set([(e[0],) for e in d["origin_farms"] if e[0] is not None]))

    if len(d["origin_regions"]) > 0 and len(d["origin_countries"]) == 0:
        d["origin_countries"] = list(set([(e[0],) for e in d["origin_regions"] if e[0] is not None]))

    # Normalize to flat lists of names
    d["origin_countries"] = list(set([e[0] for e in d["origin_countries"] if isinstance(e, tuple) and len(e) > 0 and e[0] is not None] + [e for e in d["origin_countries"] if not isinstance(e, tuple) and e is not None]))
    d["origin_regions"] = list(set([e[1] for e in d["origin_regions"] if isinstance(e, tuple) and len(e) > 1 and e[1] is not None] + [e for e in d["origin_regions"] if not isinstance(e, tuple) and e is not None]))
    d["origin_farms"] = list(set([e[2] for e in d["origin_farms"] if isinstance(e, tuple) and len(e) > 2 and e[2] is not None] + [e for e in d["origin_farms"] if not isinstance(e, tuple) and e is not None]))
    d["coffee_producers"] = list(set([e[3] for e in d["coffee_producers"] if isinstance(e, tuple) and len(e) > 3 and e[3] is not None] + [e for e in d["coffee_producers"] if not isinstance(e, tuple) and e is not None]))

    # Heuristics to drop noisy results
    if len(d["origin_countries"]) > 3:
        d["origin_countries"] = []
    if len(d["origin_regions"]) > 3:
        d["origin_regions"] = []
    if len(d["varieties"]) >= 10:
        d["varieties"] = []

    return {k: v for k, v in d.items() if v is not None}
