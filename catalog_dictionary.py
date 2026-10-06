"""Versioned vocabulary for the MÙA fashion analytics catalog."""
import re
import unicodedata

VERSION = 1
TAXONOMY = {
    "tops": {
        "t-shirt": ["basic-t-shirt", "graphic-t-shirt", "oversized-t-shirt", "long-sleeve-t-shirt"],
        "polo": ["classic-polo", "oversized-polo", "long-sleeve-polo"],
        "shirt": ["casual-shirt", "formal-shirt", "denim-shirt", "short-sleeve-shirt"],
        "hoodie": [], "sweatshirt": [], "sweater": [], "tank-top": [], "cardigan": [],
    },
    "bottoms": {
        "trousers": ["straight-trousers", "wide-leg-trousers", "slim-trousers"],
        "jeans": ["straight-jeans", "slim-jeans", "baggy-jeans", "ripped-jeans"],
        "cargo": ["cargo-pants", "cargo-shorts"],
        "shorts": ["denim-shorts", "casual-shorts", "athletic-shorts"],
        "joggers": [], "sweatpants": [],
    },
    "outerwear": {
        "jacket": ["denim-jacket", "bomber-jacket", "varsity-jacket", "leather-jacket", "windbreaker", "puffer-jacket"],
        "coat": ["trench-coat", "overcoat", "wool-coat"],
        "blazer": [], "vest": [], "overshirt": [],
    },
    "activewear": {key: [] for key in ["sports-t-shirt", "sports-shorts", "track-pants", "track-jacket", "compression-wear", "gym-wear"]},
    "accessories": {
        "hat": ["cap", "bucket-hat", "beanie"],
        "bag": ["backpack", "tote-bag", "crossbody-bag", "shoulder-bag"],
        "belt": [], "wallet": [], "scarf": [], "socks": [], "sunglasses": [], "gloves": [],
        "jewelry": ["necklace", "bracelet", "ring"],
    },
    "footwear": {
        "sneakers": ["low-top-sneakers", "high-top-sneakers", "running-sneakers"],
        "boots": [], "sandals": [], "loafers": [], "slides": [],
    },
}
COLLECTIONS = "new-arrivals best-sellers trending essentials streetwear minimal vintage office-wear summer-collection".split()
VOCABULARY = {
    "gender": "male female unisex kids".split(),
    "style": "basic minimal streetwear casual smart-casual formal sport vintage retro y2k preppy workwear techwear athleisure old-money korean oversized classic".split(),
    "material": "cotton organic-cotton polyester nylon denim linen wool leather faux-leather rayon viscose spandex acrylic silk canvas fleece".split(),
    "fit": "slim regular relaxed oversized boxy loose straight wide tapered cropped".split(),
    "color": "black white gray silver red burgundy pink orange yellow green olive khaki blue navy sky-blue purple brown beige cream multicolor".split(),
    "size": "XXS XS S M L XL XXL 3XL 4XL 5XL ONE_SIZE".split(),
    "season": "spring summer autumn winter all-season rainy-season".split(),
}


def token(value):
    text = unicodedata.normalize("NFKD", str(value).strip().lower().replace("đ", "d"))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


ALIASES = {
    "gender": {"nam": "male", "men": "male", "man": "male", "nu": "female", "women": "female", "woman": "female", "nam-nu": "unisex", "tre-em": "kids"},
    "style": {"oversize": "oversized", "street": "streetwear", "sports": "sport", "toi-gian": "minimal"},
    "fit": {"oversize": "oversized", "rong": "loose", "om": "slim", "wide-leg": "wide"},
    "color": {
        "den": "black", "jet-black": "black", "washed-black": "black", "ink": "black",
        "trang": "white", "snow-white": "white", "bone": "cream", "natural": "cream",
        "kem": "cream", "off-white": "cream", "xanh-navy": "navy", "navy-blue": "navy",
        "xam": "gray", "grey": "gray", "charcoal": "gray", "stone": "gray",
        "do": "red", "hong": "pink", "cam": "orange", "vang": "yellow",
        "xanh-la": "green", "sage": "green", "xanh-duong": "blue", "indigo": "blue",
        "nau": "brown", "sand": "beige", "tim": "purple", "nhieu-mau": "multicolor",
    },
    "size": {"os": "ONE_SIZE", "one-size": "ONE_SIZE", "onesize": "ONE_SIZE", "free-size": "ONE_SIZE", "freesize": "ONE_SIZE", "2xl": "XXL"},
    "season": {"xuan": "spring", "he": "summer", "thu": "autumn", "fall": "autumn", "dong": "winter", "bon-mua": "all-season", "mua-mua": "rainy-season"},
    "material": {"vai-cotton": "cotton", "bong": "cotton", "vai-lanh": "linen", "len": "wool", "merino-wool": "wool", "da": "leather", "da-nhan-tao": "faux-leather"},
    "brand": {"mua-official": "mua"},
}
CATEGORY_ALIASES = {
    "tops-thun": "t-shirt", "ao-thun": "t-shirt", "tee": "t-shirt",
    "tops-polo": "polo", "tops-so-mi": "shirt", "so-mi": "shirt",
    "bottoms-quan-dai": "trousers", "bottoms-denim": "jeans",
    "outerwear-jacket": "jacket", "outerwear-overshirt": "overshirt",
    "accessories-mu": "hat", "accessories-tui": "bag",
}
# Explicit, reviewable mappings for the current seed catalog; not generic name guessing.
SEED_CATEGORIES = {
    "boxy-heavy-tee": ("tops", "t-shirt", None),
    "cropped-knit-polo": ("tops", "polo", None),
    "linen-camp-shirt": ("tops", "shirt", None),
    "oxford-overshirt": ("outerwear", "overshirt", None),
    "pleated-wide-trouser": ("bottoms", "trousers", "wide-leg-trousers"),
    "straight-denim": ("bottoms", "jeans", "straight-jeans"),
    "parachute-cargo-pant": ("bottoms", "cargo", "cargo-pants"),
    "coach-jacket": ("outerwear", "jacket", None),
    "wool-beanie": ("accessories", "hat", "beanie"),
    "canvas-tote-bag": ("accessories", "bag", "tote-bag"),
    "knit-cardigan-scheduled": ("tops", "cardigan", None),
}


def category_paths():
    paths = {}
    for l1, children in TAXONOMY.items():
        paths[l1] = (l1, None, None)
        for l2, leaves in children.items():
            paths[l2] = (l1, l2, None)
            for l3 in leaves:
                paths[l3] = (l1, l2, l3)
    return paths


def normalize_value(field, value, footwear=False):
    key = token(value)
    if field == "brand":
        return ALIASES["brand"].get(key, key) or None
    if field == "size" and footwear and re.fullmatch(r"\d{2}(?:\.5)?", str(value).strip()):
        return str(value).strip()
    candidates = {token(v): v for v in VOCABULARY[field]}
    return ALIASES.get(field, {}).get(key, candidates.get(key))
