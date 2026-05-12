from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Listing:
    source: str
    city: str
    external_id: Optional[str]
    web_link: Optional[str]
    link_to_gmap: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    district: Optional[str]
    postal_code: Optional[str]
    address: Optional[str]
    available_surface_m2: Optional[float]
    floor: Optional[str]
    status: Optional[str]
    is_exterior: Optional[bool]
    has_lift: Optional[bool]
    has_air_conditioning: Optional[bool]
    price_monthly: Optional[float]
    price_per_m2: Optional[float]
    currency: Optional[str]
    energy_class: Optional[str]
    first_listed_date: Optional[str]
    last_updated_date: Optional[str]
    days_on_market: Optional[int]
    # Scraper-side contact fields
    contact_name: Optional[str]
    company_name: Optional[str]
    phone: Optional[str]
    contact_type: Optional[str]
    email: str = ""
    agency_comment: Optional[str] = None

    def matching_name(self) -> Optional[str]:
        """Agency identifier used to join listings with enriched contacts."""
        return self.company_name if self.source in ("otodom", "immobilienscout") else self.contact_name

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "city": self.city,
            "external_id": self.external_id,
            "web_link": self.web_link,
            "link_to_gmap": self.link_to_gmap,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "district": self.district,
            "postal_code": self.postal_code,
            "address": self.address,
            "available_surface_m2": self.available_surface_m2,
            "floor": self.floor,
            "status": self.status,
            "is_exterior": self.is_exterior,
            "has_lift": self.has_lift,
            "has_air_conditioning": self.has_air_conditioning,
            "price_monthly": self.price_monthly,
            "price_per_m2": self.price_per_m2,
            "currency": self.currency,
            "energy_class": self.energy_class,
            "first_listed_date": self.first_listed_date,
            "last_updated_date": self.last_updated_date,
            "days_on_market": self.days_on_market,
            "contact_name": self.contact_name,
            "company_name": self.company_name,
            "phone": self.phone,
            "contact_type": self.contact_type,
            "email": self.email,
            "agency_comment": self.agency_comment,
        }

    @staticmethod
    def from_dict(d: dict) -> "Listing":
        return Listing(**{k: d.get(k) for k in Listing.__dataclass_fields__})  # type: ignore[attr-defined]


@dataclass
class Contact:
    full_name: str
    job_title: str
    email: str
    email_confidence: str
    linkedin_url: str
    seniority_rank: int
    domain_rank: int

    def to_dict(self) -> dict:
        return {
            "full_name": self.full_name,
            "job_title": self.job_title,
            "email": self.email,
            "email_confidence": self.email_confidence,
            "linkedin_url": self.linkedin_url,
            "seniority_rank": self.seniority_rank,
            "domain_rank": self.domain_rank,
        }

    @staticmethod
    def from_dict(d: dict) -> "Contact":
        return Contact(**d)


@dataclass
class Agency:
    company_name: str
    first_name: str = ""
    last_name: str = ""
    source: str = ""
    allow_company_fallback: bool = True
    contacts: list[Contact] = field(default_factory=list)

    def is_individual(self) -> bool:
        return bool(self.first_name and self.last_name)

    def to_dict(self) -> dict:
        return {
            "company_name": self.company_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "source": self.source,
            "allow_company_fallback": self.allow_company_fallback,
            "contacts": [
                c if isinstance(c, dict) else c.to_dict() for c in self.contacts
            ],
        }

    @staticmethod
    def from_dict(d: dict) -> "Agency":
        contacts = [Contact.from_dict(c) for c in d.get("contacts", [])]
        return Agency(
            company_name=d["company_name"],
            first_name=d.get("first_name", ""),
            last_name=d.get("last_name", ""),
            source=d.get("source", ""),
            allow_company_fallback=d.get("allow_company_fallback", True),
            contacts=contacts,
        )


@dataclass
class ContactBundle:
    """Top-3 Lusha contacts for one agency, ready to write to SQL."""
    agency_name: str
    email_1: str = ""
    email_1_contact: str = ""
    email_1_title: str = ""
    email_1_confidence: str = ""
    email_1_linkedin: str = ""
    email_2: str = ""
    email_2_contact: str = ""
    email_2_title: str = ""
    email_2_confidence: str = ""
    email_2_linkedin: str = ""
    email_3: str = ""
    email_3_contact: str = ""
    email_3_title: str = ""
    email_3_confidence: str = ""
    email_3_linkedin: str = ""

    def to_dict(self) -> dict:
        return {
            "agency_name": self.agency_name,
            "email_1": self.email_1,
            "email_1_contact": self.email_1_contact,
            "email_1_title": self.email_1_title,
            "email_1_confidence": self.email_1_confidence,
            "email_1_linkedin": self.email_1_linkedin,
            "email_2": self.email_2,
            "email_2_contact": self.email_2_contact,
            "email_2_title": self.email_2_title,
            "email_2_confidence": self.email_2_confidence,
            "email_2_linkedin": self.email_2_linkedin,
            "email_3": self.email_3,
            "email_3_contact": self.email_3_contact,
            "email_3_title": self.email_3_title,
            "email_3_confidence": self.email_3_confidence,
            "email_3_linkedin": self.email_3_linkedin,
        }

    @staticmethod
    def from_dict(d: dict) -> "ContactBundle":
        return ContactBundle(**d)


@dataclass
class SourceConfig:
    city: str
    country: str
    country_code: str
    start_url: str
    actor: str
    actor_id: str
    run_id: str
    unlimited_items: bool = False

    def to_dict(self) -> dict:
        return {
            "city": self.city,
            "country": self.country,
            "country_code": self.country_code,
            "start_url": self.start_url,
            "actor": self.actor,
            "actor_id": self.actor_id,
            "run_id": self.run_id,
            "unlimited_items": self.unlimited_items,
        }

    @staticmethod
    def from_dict(d: dict) -> "SourceConfig":
        return SourceConfig(**d)


@dataclass
class RunStats:
    run_id: str
    city: str
    source: str
    buildings_found: int
    buildings_new: int
    buildings_updated: int

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "city": self.city,
            "source": self.source,
            "buildings_found": self.buildings_found,
            "buildings_new": self.buildings_new,
            "buildings_updated": self.buildings_updated,
        }
