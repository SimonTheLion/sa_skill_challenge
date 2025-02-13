import requests
import logging
import json
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
API_KEY = "KLAVIYO_PRIVATE_KEY"  # Replace with your API key
SEGMENT_ID = "KLAVIYO_SEGMENT_ID"  # Replace with your Klaviyo segment ID
SEGMENT_NAME = "Segment_Triggered_Event"  # Replace with your Klaviyo segment name
CACHE_FILE = "cache.json"

def fetch_profiles():
    """Fetch profiles from a Klaviyo Segment."""
    url = f"https://a.klaviyo.com/api/segments/{SEGMENT_ID}/profiles"
    headers = {
        "Authorization": f"Klaviyo-API-Key {API_KEY}",
        "accept": "application/vnd.api+json",
        "revision": "2025-01-15"
    }
    profiles = []
    
    params = {
        "page[size]": 100  # Fetch up to 100 profiles per page
    }
    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            profiles.extend([p["attributes"]["email"] for p in data.get("data", [])])
            url = data.get("links", {}).get("next")  # Get the next page URL
        else:
            logging.error(f"Failed to fetch profiles: {response.status_code} - {response.text}")
            return []
    
    return profiles

def push_event_to_klaviyo(email, event_name):
    """Send an event to Klaviyo for a specific email."""
    url = "https://a.klaviyo.com/api/events"
    headers = {
        "Authorization": f"Klaviyo-API-Key {API_KEY}",
        "Content-Type": "application/vnd.api+json",
        "accept": "application/vnd.api+json",
        "revision": "2025-01-15"
    }
    payload = {
        "data": {
            "type": "event",
            "attributes": {
                "properties": {
                    "segment_id": SEGMENT_ID,
                    "segment_name": SEGMENT_NAME,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                "metric": {
                    "data": {
                        "type": "metric",
                        "attributes": {
                            "name": event_name
                        }
                    }
                },
                "profile": {
                    "data": {
                        "type": "profile",
                        "attributes": {
                            "email": email
                        }
                    }
                }
            }
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 202:
            logging.info(f"Event '{event_name}' successfully sent for {email}")
        else:
            logging.error(f"Failed to send event for {email}: {response.status_code}, {response.text}")
    except requests.RequestException as e:
        logging.error(f"Error sending event for {email}: {e}")

def update_cache(profiles):
    """Update the local cache with new profiles."""
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {"profiles": [], "last_updated": None}

    new_profiles = list(set(profiles) - set(cache["profiles"]))
    if new_profiles:
        logging.info(f"New profiles added: {new_profiles}")
        for email in new_profiles:
            push_event_to_klaviyo(email, "Joined Segment")

        cache["profiles"].extend(new_profiles)
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=4)
        logging.info("Local cache updated.")
    else:
        logging.info("No new profiles to add.")

def remove_stale_profiles(fetched_profiles):
    """Remove profiles from the cache that are no longer in the Klaviyo segment."""
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning("Cache file not found or invalid. Skipping stale profile removal.")
        return

    current_cached_profiles = set(cache.get("profiles", []))
    fetched_profiles_set = set(fetched_profiles)

    # Profiles to be removed
    stale_profiles = current_cached_profiles - fetched_profiles_set
    if stale_profiles:
        logging.info(f"Stale profiles removed: {list(stale_profiles)}")
        for email in stale_profiles:
            push_event_to_klaviyo(email, "Left Segment")

        cache["profiles"] = list(current_cached_profiles - stale_profiles)
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Write updated cache back to the file
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=4)
        logging.info("Stale profiles removed and cache updated.")
    else:
        logging.info("No stale profiles to remove.")

def main():
    profiles = fetch_profiles()
    if profiles:
        update_cache(profiles)
        remove_stale_profiles(profiles)
    else:
        logging.warning("No profiles fetched. Exiting script.")

if __name__ == "__main__":
    main()
