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
        d[key] = list(
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

    if len(d["coffee_producers"]) > 0:
        for p in d["coffee_producers"]:
            if p[2] is not None and len(d["origin_farms"]) > 0:
                d["origin_farms"] = list(
                    set([(e[0], e[1], e[2]) for e in d["origin_farms"] if e[2] == p[2]])
                )
            if p[1] is not None and len(d["origin_regions"]) > 0:
                d["origin_regions"] = list(
                    set([(e[0], e[1]) for e in d["origin_regions"] if e[1] == p[1]])
                )
            if p[0] is not None and len(d["origin_countries"]) > 0:
                d["origin_countries"] = list(
                    set([(e[0],) for e in d["origin_countries"] if e[0] == p[0]])
                )

    if len(d["origin_farms"]) > 0:
        for f in d["origin_farms"]:
            if f[1] is not None and len(d["origin_regions"]) > 0:
                d["origin_regions"] = list(
                    set([(e[0], e[1]) for e in d["origin_regions"] if e[1] == f[1]])
                )
            if f[0] is not None and len(d["origin_countries"]) > 0:
                d["origin_countries"] = list(
                    set([(e[0],) for e in d["origin_countries"] if e[0] == f[0]])
                )

    if len(d["origin_farms"]) > 0:
        if len(d["origin_regions"]) == 0:
            d["origin_regions"] = list(
                set(
                    [
                        (
                            e[0],
                            e[1],
                        )
                        for e in d["origin_farms"]
                    ]
                )
            )
        if len(d["origin_countries"]) == 0:
            d["origin_countries"] = list(set([(e[0],) for e in d["origin_farms"]]))

    if len(d["origin_regions"]) > 0:
        if len(d["origin_countries"]) == 0:
            d["origin_countries"] = list(set([(e[0],) for e in d["origin_regions"]]))

    d["origin_countries"] = list(set([e[0] for e in d["origin_countries"]]))
    d["origin_regions"] = list(set([e[1] for e in d["origin_regions"]]))
    d["origin_farms"] = list(set([e[2] for e in d["origin_farms"]]))
    d["coffee_producers"] = list(set([e[3] for e in d["coffee_producers"]]))

    if len(d["origin_countries"]) > 3:
        d["origin_countries"] = []
    if len(d["origin_regions"]) > 3:
        d["origin_regions"] = []
    if len(d["varieties"]) >= 10:
        d["varieties"] = []

    return {k: v for k, v in d.items() if v is not None}
