from text import strip_accents
# variety_regexp = re.compile(r'\b(guara|marshell|heirloom|peaberry|74110|obata|h1|jaadi|sudan(?: rume)|maracaturra|lempira|line s|catimor|java|laurina|icatu|villa sarchi|tabi|wush[-\s]?wush|S(?:-)?795|Sigarar utang|Adungsari|Kartika|ethiosar|sidra|pacas|parainema|san roman|costa rica\s?95|colombia|sarchimore|ombligon|(?:yellow)? icatu|jarc|arara|(?:(?:yellow|red|pink)\s*)?catua[iìíïÍ]|caturra|chiroso|castillo|mundo|gesha|geisha|(?:(?:yellow|red|pink)\s*)?bourbon(?:\s*(?:jaune|rouge|rose|pointu))?|typica|dega|kudhume|wolisho|sl\s?[0-9]+|batian|ruiru (?:[0-9]+)?|heirloom|pacamara)\b', flags=re.IGNORECASE)


class VarietiesValidator:
    def key(self):
        return "COFFEE_VARIETY"

    def validate(self, v):
        elt = (
            strip_accents(v.lower())
            .replace("geisha", "gesha")
            .replace("ethiopian landrace", "heirloom")
            .strip()
        )
        return elt
