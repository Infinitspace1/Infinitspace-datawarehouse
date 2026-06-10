"""Fetch an Apify run's log and grep for how the actor handled the start URL."""
import os
import re
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from apify_client import ApifyClient

RUN_ID = sys.argv[1] if len(sys.argv) > 1 else "G2cYzUV8faBZ4OXQh"

client = ApifyClient(os.environ["APIFY_TOKEN"])
log_text = client.run(RUN_ID).log().get() or ""
print(f"log length: {len(log_text)}")

interesting = []
for line in log_text.splitlines():
    if re.search(r"url|URL|filter|size|500|moreResults|results|page|total", line):
        interesting.append(line)

# Head of log + interesting lines, deduped, capped
print("\n--- first 40 lines ---")
for line in log_text.splitlines()[:40]:
    print(line)

print("\n--- interesting lines (max 80) ---")
seen = set()
count = 0
for line in interesting:
    key = re.sub(r"\d", "#", line)[:120]
    if key in seen:
        continue
    seen.add(key)
    print(line[:300])
    count += 1
    if count >= 80:
        break
