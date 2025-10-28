"""
Faker producer with Pydantic validation.

Generates batches of JSON-line events, validates each record
using Pydantic models, and uploads gzipped JSON-lines to S3.

Install Dependencies:
    pip install boto3 faker pydantic==2.12.3
"""

import gzip
import json
import os
import random
import string
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Literal, Optional

import boto3
from faker import Faker
from pydantic import BaseModel, ValidationError, field_validator, model_validator

# --------------------------------------------------
# CONFIG (env or defaults)
# --------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET", "swiggy-data-generation")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/events")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000000"))  # rows per file
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "5"))  # wait between uploads
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
MAX_UNIQUENESS_ATTEMPTS = 20

fake = Faker("en_IN")
s3 = boto3.client("s3", region_name=AWS_REGION)


# --------------------------------------------------
# Global Uniqueness Trackers
# --------------------------------------------------
# It ensures that no duplicate customers / agents / restaurants / menus are included.
existing_customer_mobiles: set = set()
existing_customer_emails: set = set()
existing_customer_name_dob: set = set()
existing_agent_phones: set = set()
existing_restaurant_keys: set = set()
existing_menu_keys: set = set()


# --------------------------------------------------
# Utilities
# --------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def random_phone_india() -> str:
    # Indian phone numbers are 10 digits long, starting with 6-9.
    return random.choice("6789") + "".join(random.choices(string.digits, k=9))


def to_decimal(v) -> Decimal:
    # Helper to produce a Decimal with 2 decimal places.
    return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# --------------------------------------------------
# Pydantic Models & Validators
# --------------------------------------------------
class CustomerModel(BaseModel):
    type: Literal["customer"] = "customer"
    customer_id: str
    name: str
    mobile: str
    email: str
    loginbyusing: Optional[str]
    gender: Optional[str]
    dob: Optional[str]
    preferences: Optional[Dict]
    created_date: str

    @field_validator("mobile")
    @classmethod
    def validate_mobile(cls, v: str) -> str:
        if len(v) != 10 or v[0] not in "6789" or not v.isdigit():
            raise ValueError("Invalid Mobile Number")
        return v

    @field_validator("created_date", "dob", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class AddressModel(BaseModel):
    type: Literal["customeraddressbook"] = "customeraddressbook"
    address_id: str
    customer_id: str
    flatno: Optional[str]
    houseno: Optional[str]
    floor: Optional[str]
    building: Optional[str]
    landmark: Optional[str]
    coordinates: Optional[str]
    primaryflag: Optional[str]
    address_type: Optional[str]
    locality: Optional[str]
    city: Optional[str]
    state: Optional[str]
    pincode: Optional[int]
    created_date: str

    @field_validator("coordinates")
    @classmethod
    def validate_coordinates(cls, v: Optional[str]) -> Optional[str]:
        if v:
            parts = v.split(",")
            if len(parts) != 2:
                raise ValueError("Coordinates must be 'lat,lon'")
            lat, lon = map(float, parts)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError("Coordinates out of range")
        return v

    @field_validator("pincode")
    @classmethod
    def validate_pincode(cls, v: Optional[int]) -> Optional[int]:
        if v and (v < 100000 or v > 999999):
            raise ValueError("Pincode must be 6 digits")
        return v

    @field_validator("created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class LocationModel(BaseModel):
    type: Literal["location"] = "location"
    location_id: str
    city: str
    state: str
    zipcode: Optional[str]
    activeflag: Optional[str]
    created_date: str

    @field_validator("created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class RestaurantModel(BaseModel):
    type: Literal["restaurant"] = "restaurant"
    restaurant_id: str
    name: str
    cuisine_type: Optional[str]
    pricing_for_2: Decimal
    location_id: Optional[str]
    created_date: str

    @field_validator("name")
    @classmethod
    def name_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Restaurant name cannot be empty")
        if len(v) > 200:
            raise ValueError("Restaurant name too long")
        return v

    @field_validator("pricing_for_2")
    @classmethod
    def validate_price(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("Price must be positive")
        return to_decimal(v)

    @field_validator("created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class MenuModel(BaseModel):
    type: Literal["menu"] = "menu"
    menu_id: str
    restaurant_id: str
    itemname: str
    description: Optional[str]
    price: Decimal
    activeflag: Optional[str]
    created_date: str

    @field_validator("itemname")
    @classmethod
    def itemname_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Item name cannot be empty")
        if len(v) > 150:
            raise ValueError("Item name too long")
        return v

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("Price must be positive")
        return to_decimal(v)

    @field_validator("created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class OrderItemModel(BaseModel):
    type: Literal["orderitem"] = "orderitem"
    orderitem_id: str
    order_id: str
    menu_id: str
    quantity: int
    price: Decimal
    subtotal: Decimal

    @field_validator("quantity", mode="before")
    @classmethod
    def quantity_positive(cls, v: int) -> int:
        if int(v) <= 0:
            raise ValueError("Quantity must be > 0")
        return int(v)

    @field_validator("price", "subtotal", mode="before")
    @classmethod
    def decimal_positive(cls, v: Decimal) -> Decimal:
        d = to_decimal(v)
        if d < 0:
            raise ValueError("Price/Subtotal must be non-negative")
        return d

    @model_validator(mode="after")
    def check_subtotal(self) -> "OrderItemModel":
        expected = (self.price * self.quantity).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if self.subtotal != expected and self.subtotal < expected - Decimal("0.01"):
            raise ValueError(
                f"Subtotal does not match price*quantity (expected {expected}, got {self.subtotal})"
            )
        return self


class OrderModel(BaseModel):
    type: Literal["orders"] = "orders"
    order_id: str
    customer_id: str
    restaurant_id: str
    order_date: str
    totalamount: Decimal
    status: Optional[str]
    paymentmethod: Optional[str]
    created_date: str

    @field_validator("order_date", "created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v

    @field_validator("totalamount", mode="before")
    @classmethod
    def total_positive(cls, v: Decimal) -> Decimal:
        d = to_decimal(v)
        if d < 0:
            raise ValueError("Total amount must be non-negative")
        return d


class DeliveryAgentModel(BaseModel):
    type: Literal["deliveryagent"] = "deliveryagent"
    deliveryagent_id: str
    name: str
    phone: str
    vehicle_type: Optional[str]
    location_id: Optional[str]
    status: Optional[str]
    rating: Optional[Decimal]
    created_date: str

    @field_validator("phone")
    @classmethod
    def validate_mobile(cls, v: str) -> str:
        if len(v) != 10 or v[0] not in "6789" or not v.isdigit():
            raise ValueError("Invalid Mobile Number")
        return v

    @field_validator("rating", mode="before")
    @classmethod
    def rating_ok(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is None:
            return v
        d = to_decimal(v)
        if d < 0 or d > 5:
            raise ValueError("Rating must be between 0 and 5")
        return d

    @field_validator("created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class DeliveryModel(BaseModel):
    type: Literal["delivery"] = "delivery"
    delivery_id: str
    order_id: str
    deliveryagent_id: str
    deliverystatus: Optional[str]
    estimated_time: Optional[str]
    address_id: str
    delivery_date: Optional[str]
    created_date: str

    @field_validator("delivery_date", "created_date", mode="before")
    @classmethod
    def validate_iso(cls, v: str) -> str:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


class LoginAuditModel(BaseModel):
    type: Literal["loginaudit"] = "loginaudit"
    login_id: str
    customer_id: str
    logintype: Optional[str]
    deviceinterface: Optional[str]
    mobiledevicename: Optional[str]
    webinterface: Optional[str]
    lastlogin: Optional[str]

    @field_validator("lastlogin", mode="before")
    @classmethod
    def lastlogin_ok(cls, v):
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("Invalid ISO timestamp")
        return v


# --------------------------------------------------
# Generator functions using Faker + Validation
# --------------------------------------------------
def make_customer_candidate(customer_id=None) -> dict:
    cid = customer_id or str(uuid.uuid4())
    name = fake.name()
    domain = random.choice(["gmail", "outlook", "hotmail", "icloud"])
    email = f"{name.lower().strip()}{str(random.randint(1, 9999))}@{domain}.com"

    payload = {
        "type": "customer",
        "customer_id": cid,
        "name": name,
        "mobile": random_phone_india(),
        "email": email,
        "loginbyusing": random.choice(["OTP", "Google", "Facebook", "Email"]),
        "gender": random.choice(["Male", "Female", "Other"]),
        "dob": fake.date_of_birth(minimum_age=10, maximum_age=90).isoformat(),
        "preferences": {
            "vegan": random.choice([True, False]),
            "spicy": random.choice(["low", "medium", "high"]),
        },
        "created_date": now_iso(),
    }
    return payload


def make_customer_unique(customer_id: Optional[str] = None) -> Optional[dict]:
    """
    Create a customer ensuring uniqueness on mobile and email (and name+dob).
    If collisions occur, try regenerating up to MAX_UNIQUENESS_ATTEMPTS.
    """
    for _ in range(MAX_UNIQUENESS_ATTEMPTS):
        payload = make_customer_candidate(customer_id)
        mobile = payload["mobile"]
        email = payload["email"].lower()
        name_dob = (payload["name"].strip().lower(), payload["dob"])

        # If mobile or email already exists, regenerate fields rather than creating duplicates.
        if (
            mobile in existing_customer_mobiles
            or email in existing_customer_emails
            or name_dob in existing_customer_name_dob
        ):
            continue

        valid = validate_or_log(payload, CustomerModel)
        if not valid:
            continue

        # Register uniqueness.
        existing_customer_mobiles.add(valid["mobile"])
        existing_customer_emails.add(valid["email"].lower())
        existing_customer_name_dob.add(
            (valid["name"].strip().lower(), valid.get("dob"))
        )
        return valid

    print("[WARNING] Unable to create a unique customer after attempts; skipping.")
    return None


def make_address(customer_id):
    payload = {
        "type": "customeraddressbook",
        "address_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "flatno": str(random.randint(1, 500)),
        "houseno": str(random.randint(1, 2000)),
        "floor": random.randint(0, 40),
        "building": fake.street_name(),
        "landmark": fake.street_address(),
        "coordinates": f"{fake.latitude()},{fake.longitude()}",
        "primaryflag": random.choice(["Y", "N"]),
        "address_type": random.choice(["Home", "Work"]),
        "locality": fake.city_suffix(),
        "city": fake.city(),
        "state": fake.state(),
        "pincode": (
            int(fake.postcode())
            if fake.postcode().isdigit() and len(fake.postcode()) == 6
            else random.randint(100000, 999999)
        ),
        "created_date": now_iso(),
    }
    return validate_or_log(payload, AddressModel)


def make_location():
    payload = {
        "type": "location",
        "location_id": str(uuid.uuid4()),
        "city": fake.city(),
        "state": fake.state(),
        "zipcode": (
            fake.postcode()
            if fake.postcode().isdigit() and len(fake.postcode()) == 6
            else str(random.randint(100000, 999999))
        ),
        "activeflag": random.choice(["Y", "N"]),
        "created_date": now_iso(),
    }
    return validate_or_log(payload, LocationModel)


def make_restaurant_unique() -> Optional[dict]:
    """
    Create a restaurant ensuring uniqueness on (name, location_id).
    """
    for _ in range(MAX_UNIQUENESS_ATTEMPTS):
        location_id = str(uuid.uuid4())
        name = fake.company()
        key = (name.strip().lower(), location_id)
        if key in existing_restaurant_keys:
            continue

        payload = {
            "type": "restaurant",
            "restaurant_id": str(uuid.uuid4()),
            "name": name,
            "cuisine_type": random.choice(
                ["Indian", "Chinese", "Italian", "Fast Food", "Mexican"]
            ),
            "pricing_for_2": str(round(random.uniform(100, 2000), 2)),
            "location_id": location_id,
            "created_date": now_iso(),
        }
        valid = validate_or_log(payload, RestaurantModel)
        if not valid:
            continue

        existing_restaurant_keys.add(
            (valid["name"].strip().lower(), valid["location_id"])
        )
        return valid

    print("[WARNING] Unable to create a unique restaurant after attempts; skipping.")
    return None


def make_menu_unique(restaurant_id: str) -> Optional[dict]:
    """
    Create a menu item ensuring uniqueness on (restaurant_id, itemname).
    """
    for _ in range(MAX_UNIQUENESS_ATTEMPTS):
        itemname = (
            fake.word().title() + " " + (getattr(fake, "food", lambda: "Item")())
        ).strip()
        key = (restaurant_id, itemname.strip().lower())
        if key in existing_menu_keys:
            continue

        payload = {
            "type": "menu",
            "menu_id": str(uuid.uuid4()),
            "restaurant_id": restaurant_id,
            "itemname": itemname,
            "description": fake.sentence(nb_words=8),
            "price": str(round(random.uniform(50, 800), 2)),
            "activeflag": random.choice(["Y", "N"]),
            "created_date": now_iso(),
        }
        valid = validate_or_log(payload, MenuModel)
        if not valid:
            continue

        existing_menu_keys.add(
            (valid["restaurant_id"], valid["itemname"].strip().lower())
        )
        return valid

    print("[WARNING] Unable to create a unique menu item after attempts; skipping.")
    return None


def make_order_with_items(customer_id, restaurant_id, menus_for_rest):
    order_id = str(uuid.uuid4())
    if not menus_for_rest:
        return None, []

    chosen = random.sample(
        menus_for_rest, k=random.randint(1, min(3, len(menus_for_rest)))
    )
    items = []

    for menu_item in chosen:
        qty = random.randint(1, 3)
        price = Decimal(str(menu_item["price"]))
        subtotal = (price * qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        itm_payload = {
            "type": "orderitem",
            "orderitem_id": str(uuid.uuid4()),
            "order_id": order_id,
            "menu_id": menu_item["menu_id"],
            "quantity": qty,
            "price": str(price),
            "subtotal": str(subtotal),
        }
        itm_valid = validate_or_log(itm_payload, OrderItemModel)
        if itm_valid:
            items.append(itm_valid)

    if not items:
        return None, []

    total = sum(Decimal(itm["subtotal"]) for itm in items) + to_decimal(
        random.uniform(10, 60)
    )

    order_payload = {
        "type": "orders",
        "order_id": order_id,
        "customer_id": customer_id,
        "restaurant_id": restaurant_id,
        "order_date": now_iso(),
        "totalamount": str(total),
        "status": random.choice(["placed", "preparing", "on_the_way", "delivered"]),
        "paymentmethod": random.choice(["card", "cash", "wallet", "upi"]),
        "created_date": now_iso(),
    }
    order_valid = validate_or_log(order_payload, OrderModel)
    return order_valid, items


def make_deliveryagent_unique() -> Optional[dict]:
    """
    Create a delivery agent ensuring phone uniqueness.
    """
    for _ in range(MAX_UNIQUENESS_ATTEMPTS):
        phone = random_phone_india()
        if phone in existing_agent_phones:
            continue

        payload = {
            "type": "deliveryagent",
            "deliveryagent_id": str(uuid.uuid4()),
            "name": fake.name(),
            "phone": phone,
            "vehicle_type": random.choice(["bike", "scooter", "car"]),
            "location_id": str(uuid.uuid4()),
            "status": random.choice(["available", "busy", "offline"]),
            "rating": str(round(random.uniform(1.0, 5.0), 1)),
            "created_date": now_iso(),
        }
        valid = validate_or_log(payload, DeliveryAgentModel)
        if not valid:
            continue

        existing_agent_phones.add(valid["phone"])
        return valid

    print(
        "[WARNING] Unable to create a unique delivery agent after attempts; skipping."
    )
    return None


def make_delivery(order_id, deliveryagent_id, address_id):
    payload = {
        "type": "delivery",
        "delivery_id": str(uuid.uuid4()),
        "order_id": order_id,
        "deliveryagent_id": deliveryagent_id,
        "deliverystatus": random.choice(["assigned", "picked_up", "delivered"]),
        "estimated_time": f"00:{random.randint(10,40):02d}:00",
        "address_id": address_id,
        "delivery_date": now_iso(),
        "created_date": now_iso(),
    }
    return validate_or_log(payload, DeliveryModel)


def make_loginaudit(customer_id):
    payload = {
        "type": "loginaudit",
        "login_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "logintype": random.choice(["web", "mobile"]),
        "deviceinterface": random.choice(["iOS", "Android", "Chrome", "Firefox"]),
        "mobiledevicename": fake.user_agent(),
        "webinterface": fake.user_agent(),
        "lastlogin": now_iso(),
    }
    return validate_or_log(payload, LoginAuditModel)


# --------------------------------------------------
# Validation Helper
# --------------------------------------------------
def validate_or_log(payload: dict, model_cls):
    """
    Attempt to validate payload with model_cls.
    If valid, return model.dict() (with native types like Decimal -> str for JSON).
    If invalid, log and return None.
    """
    try:
        model = model_cls.model_validate(payload)
        d = model.model_dump()
        # Convert top-level Decimal to string for JSON compatibility.
        for k, v in list(d.items()):
            if isinstance(v, Decimal):
                d[k] = format(v, "f")
        return d
    except ValidationError as exc:
        print(
            f"[VALIDATION FAILED] model={model_cls.__name__} id={payload.get(list(payload.keys())[1], 'unknown')} errors={exc.errors()}",
            file=sys.stderr,
        )
        return None


# --------------------------------------------------
# Batch Builder & Uploader
# --------------------------------------------------
def build_batch(batch_size: int) -> List[dict]:
    rows: List[dict] = []

    # Create stable pools (each entity is produced once and reused).
    num_customers = max(10, batch_size // 20)
    num_restaurants = max(5, batch_size // 50)
    num_agents = max(5, batch_size // 50)

    customers: List[dict] = []

    # Generate unique customers.
    attempts = 0
    while len(customers) < num_customers and attempts < num_customers * 10:
        attempts += 1
        c = make_customer_unique()
        if c:
            customers.append(c)
            rows.append(c)

    addresses: List[dict] = []
    for c in customers:
        # Create 1-2 addresses per customer.
        for _ in range(random.randint(1, 2)):
            a = make_address(c["customer_id"])
            if a:
                addresses.append(a)
                rows.append(a)
        # Occasional login events.
        if random.random() < 0.4:
            la = make_loginaudit(c["customer_id"])
            if la:
                rows.append(la)

    restaurants: List[dict] = []
    for _ in range(num_restaurants):
        r = make_restaurant_unique()
        if r:
            restaurants.append(r)
            rows.append(r)

    menus: Dict[str, List[dict]] = {}
    for r in restaurants:
        mlist: List[dict] = []
        # Generate unique menus per restaurant.
        for _ in range(random.randint(3, 6)):
            m = make_menu_unique(r["restaurant_id"])
            if m:
                mlist.append(m)
                rows.append(m)
        if not mlist:
            # Fallback single menu.
            fallback = {
                "type": "menu",
                "menu_id": str(uuid.uuid4()),
                "restaurant_id": r["restaurant_id"],
                "itemname": "Basic Item",
                "description": "auto-created",
                "price": "100.00",
                "activeflag": "Y",
                "created_date": now_iso(),
            }
            # Register fallback key to keep global uniqueness consistent.
            existing_menu_keys.add(
                (fallback["restaurant_id"], fallback["itemname"].strip().lower())
            )
            mlist.append(fallback)
            rows.append(fallback)
        menus[r["restaurant_id"]] = mlist

    agents: List[dict] = []
    for _ in range(num_agents):
        a = make_deliveryagent_unique()
        if a:
            agents.append(a)
            rows.append(a)

    # Create orders until we reach the desired approximate batch size.
    # Note: Orders produce multiple rows (order, order items, and optional delivery).
    attempts = 0
    while len(rows) < batch_size and attempts < batch_size * 10:
        attempts += 1
        cust = random.choice(customers)
        rest = random.choice(restaurants)
        rest_menus = menus.get(rest["restaurant_id"], [])
        order_valid, items = make_order_with_items(
            cust["customer_id"], rest["restaurant_id"], rest_menus
        )
        if order_valid:
            rows.append(order_valid)
            rows.extend(items)

            # Optionally create a delivery for the order. Reuse delivery agents.
            if random.random() < 0.6 and agents:
                cust_addresses = [
                    a for a in addresses if a["customer_id"] == cust["customer_id"]
                ]
                address_id = None
                if cust_addresses:
                    address_id = random.choice(cust_addresses)["address_id"]
                elif addresses:
                    address_id = random.choice(addresses)["address_id"]

                agent = random.choice(agents)
                if address_id:
                    delivery_rec = make_delivery(
                        order_valid["order_id"], agent["deliveryagent_id"], address_id
                    )
                    if delivery_rec:
                        rows.append(delivery_rec)

    # If we overshot, slice to the requested size (this may cut an entity group mid-way).
    # If group integrity is required (i.e., order and its items), consider not slicing and instead
    # adjust the loop to account for the group size. Currently, we slice to ensure file size.
    if len(rows) > batch_size:
        return rows[:batch_size]

    return [r for r in rows if r is not None]


def upload_batch_to_s3(rows: List[dict]):
    if not rows:
        raise Exception("No valid rows to upload")

    datepart = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hourpart = datetime.now(timezone.utc).strftime("%H")
    key = f"{S3_PREFIX}/date={datepart}/hour={hourpart}/part-{uuid.uuid4().hex}.json.gz"

    # Prepare gzipped newline JSON.
    body_lines = []
    for r in rows:
        body_lines.append(json.dumps(r, default=str))

    body = ("\n".join(body_lines)).encode("utf-8")
    gz = gzip.compress(body)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=gz, ContentType="application/gzip")
    print(f"Uploaded {len(rows)} validated records to s3://{S3_BUCKET}/{key}")


# --------------------------------------------------
# Main loop
# --------------------------------------------------
def main():
    print("Starting faker producer with Pydantic validation. Ctrl-C to stop.")
    try:
        while True:
            rows = build_batch(BATCH_SIZE)
            upload_batch_to_s3(rows)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Stopped by User")


if __name__ == "__main__":
    main()
