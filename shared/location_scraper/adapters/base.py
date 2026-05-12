from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from shared.location_scraper.models import Listing


class SourceAdapter(Protocol):
    actor_id: str

    def build_input(self, start_url: str, max_items: int | None | str = "default") -> dict:
        """Return the Apify actor input dict for this source."""
        ...

    def normalize(self, raw_item: dict, city: str) -> "Listing | None":
        """
        Map one raw Apify dataset item to a Listing.
        Return None to skip the item (e.g. wrong city filter).
        """
        ...
