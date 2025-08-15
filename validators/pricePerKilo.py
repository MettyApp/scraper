class PricePerKiloValidator:
    def key(self):
        return "PRICE_PER_KILO"

    def validate(self, v):
        try:
            if v < 600 and v > 30:
                return v
        except Exception:
            return None
