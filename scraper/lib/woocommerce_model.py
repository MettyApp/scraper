from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field


class Link(BaseModel):
    href: str
    targetHints: Optional[Dict[str, List[str]]] = None
    name: Optional[str] = None
    templated: Optional[bool] = None
    embeddable: Optional[bool] = None
    taxonomy: Optional[str] = None


class Links(BaseModel):
    self_: List[Link] = Field(..., alias="self")
    collection: List[Link]
    about: List[Link]
    curies: List[Link]
    # champs optionnels selon le type
    wp_post_type: Optional[List[Link]] = Field(None, alias="wp:post_type")
    up: Optional[List[Link]] = None
    replies: Optional[List[Link]] = None
    wp_featuredmedia: Optional[List[Link]] = Field(None, alias="wp:featuredmedia")
    wp_attachment: Optional[List[Link]] = Field(None, alias="wp:attachment")
    wp_term: Optional[List[Link]] = Field(None, alias="wp:term")


# --------- ProductTag ---------
class ProductTag(BaseModel):
    id: int
    count: int
    description: str
    link: str
    name: str
    slug: str
    taxonomy: str
    links: Links = Field(..., alias="_links")

    class Config:
        validate_by_name = True
        populate_by_name = True

    @staticmethod
    def from_json(data: str | dict) -> "ProductTag":
        import json

        if isinstance(data, str):
            data = json.loads(data)
        return ProductTag(**data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)


# --------- Product ---------
class RenderedText(BaseModel):
    rendered: str
    protected: Optional[bool] = None


class Guid(BaseModel):
    rendered: str


class Product(BaseModel):
    id: int
    date: str
    date_gmt: str
    guid: Guid
    modified: str
    modified_gmt: str
    slug: str
    status: str
    type: str
    link: str
    title: RenderedText
    content: RenderedText
    excerpt: RenderedText
    featured_media: Optional[int] = None
    product_brand: List[Any] = []
    product_cat: List[int] = []
    product_tag: List[int] = []
    links: Links = Field(..., alias="_links")

    class Config:
        validate_by_name = True
        populate_by_name = True

    @staticmethod
    def from_json(data: str | dict) -> "Product":
        import json

        if isinstance(data, str):
            data = json.loads(data)
        return Product(**data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)


# --------- ProductCategory ---------
class ProductCategory(BaseModel):
    id: int
    count: int
    description: str
    link: str
    name: str
    slug: str
    taxonomy: str
    parent: int
    links: Links = Field(..., alias="_links")

    class Config:
        validate_by_name = True
        populate_by_name = True

    @staticmethod
    def from_json(data: str | dict) -> "ProductCategory":
        import json

        if isinstance(data, str):
            data = json.loads(data)
        return ProductCategory(**data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)


class ProductMedia(BaseModel):
    id: int
    guid: Guid

    class Config:
        validate_by_name = True
        populate_by_name = True

    @staticmethod
    def from_json(data: str | dict) -> "ProductMedia":
        import json

        if isinstance(data, str):
            data = json.loads(data)
        return ProductMedia(**data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)
