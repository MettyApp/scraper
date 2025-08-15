import collections
from enum import Enum
from typing import List
from langchain_mistralai import ChatMistralAI

from pydantic import BaseModel, Field
from bs4 import BeautifulSoup, Comment


class CoffeeProcess(str, Enum):
    washed = "washed"
    natural = "natural"
    anaerobic = "anaerobic"
    honey = "honey"
    experimental = "experimental"


class CoffeeProperties(BaseModel):
    coffee_name: str = Field(description="Coffee friendly short name.")
    is_blend: bool = Field(
        description="True if this coffee is a blend, False if it is a single origin coffee."
    )
    is_decaf: bool = Field(
        description="True if this coffee is decafeinated, False if it is not."
    )
    price_per_kilo: float = Field(description="Coffee retail price per kilo, in euros.")
    origin_countries: List[str] = Field(
        description="Coffee origin countries, empty if not specified. Each country name must be written in english, and in capital letters.",
        default=[],
    )
    origin_regions: List[str] = Field(
        description="Coffee origin regions, empty if not specified.", default=[]
    )
    origin_farms: List[str] = Field(
        description="Coffee origin farms, empty if not specified", default=[]
    )
    coffee_producers: List[str] = Field(
        description="Coffee producers, empty if not specified.", default=[]
    )
    origin_washing_station: List[str] = Field(
        description="Coffee origin washing station, empty if not specified", default=[]
    )
    processes: List[str] = Field(
        description="Coffee processing method, empty if not specified. Valid values are: 'washed', 'natural', 'anaerobic', 'honey' or 'experimental'. ",
        default=[],
    )
    altitude: List[int] = Field(description="Coffee altitude", default=[])
    varieties: List[str] = Field(
        description="Coffee varieties, empty if not specified", default=[]
    )
    tasting_notes: List[str] = Field(
        description="Coffee tasting notes, empty if not specified", default=[]
    )
    taste_profile_fruity: float = Field(
        description="""Estimates a fruitiness score based on the coffee description. The value is a float from 0 to 1, where 0 means the coffee has no fruity notes, and 1 means it is intensely fruity. Tasting notes like "berry", "citrus", or "tropical fruit" indicate high fruitiness, while "nutty" or "earthy" suggest low fruitiness.""",
        default=0.0,
        min=0.0,
        max=1.0,
    )
    taste_profile_floral: float = Field(
        description="""Estimates a floral score based on the coffee description. The value is a float from 0 to 1, where 0 means the coffee has no floral notes, and 1 means it is highly floral. Tasting notes like "jasmine", "hibiscus", or "orange blossom" indicate strong floral characteristics, while "spicy" or "caramelized" suggest a lower floral presence.""",
        default=0.0,
        min=0.0,
        max=1.0,
    )
    taste_profile_chocolate: float = Field(
        description="""Estimates a chocolatey score based on the coffee description. The value is a float from 0 to 1, where 0 means the coffee has no chocolatey notes, and 1 means it is intensely chocolatey. Tasting notes like "nutty" or "cocoa" indicate strong chocolatey characteristics, while "citrusy" or "floral" suggest a lower chocolate presence.""",
        default=0.0,
        min=0.0,
        max=1.0,
    )
    taste_profile_spicy: float = Field(
        description="""Estimates a spiciness score based on the coffee description. The value is a float from 0 to 1, where 0 means the coffee has no spicy notes, and 1 means it is intensely spicy. Tasting notes like "cinnamon", "clove", or "pepper" indicate strong spiciness, while "floral" or "fruity" suggest a lower spicy presence.""",
        default=0.0,
        min=0.0,
        max=1.0,
    )
    taste_profile_sweetness: float = Field(
        description="""Estimates a coffee sweetness score based on its description. The value is a float from 0 to 1, where 0 means the coffee has no sweetness, and 1 means it is very sweet. Tasting notes like "honey" or "agave syrup" indicate high sweetness, while "dark chocolate" or "woody" suggest lower sweetness.""",
        default=0.0,
        min=0.0,
        max=1.0,
    )


class CoffeeDataExtraction(BaseModel):
    coffees: List[CoffeeProperties] = Field(
        description="featured coffee properties", default=[]
    )


class MistralExtractor:
    classifier_name = "ft:classifier:ministral-3b-latest:309c1c30:20250723:6426b84b"

    structured_llm = ChatMistralAI(
        temperature=1, max_retries=1, model_name="mistral-small-latest"
    ).with_structured_output(CoffeeDataExtraction)

    def parse(self, url, soup):
        soup = BeautifulSoup(str(soup), "lxml")
        for tag in soup.find_all(
            [
                "script",
                "script",
                "img",
                "noscript",
                "style",
                "head",
                "svg",
                "link",
                "header",
                "footer",
                "nav",
                "aside",
                "button",
            ]
        ):
            tag.extract()
        for elt in soup(text=lambda text: isinstance(text, Comment)):
            elt.extract()

        class_counter = collections.Counter()
        for tag in soup.find_all(class_=True):
            classes = tag.get("class")
            for cls in classes:
                class_counter[cls] += 1

        for tag in soup.find_all(class_=True):
            classes = tag.get("class")
            tag["class"] = [
                cls
                for cls in classes
                if class_counter[cls] <= 3 and ":" not in cls and "[" not in cls
            ] and "elementor-" not in cls
            if not tag["class"]:
                del tag["class"]
            for attr in list(
                tag.attrs
            ):  # Copie des clés pour éviter modification en cours d'itération
                if (
                    attr.startswith("data-")
                    or attr.startswith("aria-")
                    or attr.startswith("elementor-")
                ):
                    del tag[attr]
        for tag in soup.find_all(attrs={"hidden": True}):
            tag.extract()
        for a_tag in soup.find_all("a"):
            if "href" in a_tag.attrs:
                del a_tag["href"]
        for tag in soup.find_all():
            for attr_name, attr_value in list(tag.attrs.items()):
                if isinstance(attr_value, list):
                    continue
                elif isinstance(attr_value, str) and len(attr_value) > 50:
                    del tag[attr_name]

        for div in soup.find_all():
            children = div.find_all(recursive=False)

            for child in children:
                if div.get("class") == child.get("class"):
                    child.unwrap()

        t = "".join([e for e in str(soup).split("\n") if len(e) > 0])
        return [v.model_dump() for v in self.structured_llm.invoke(t).coffees]
