from datetime import datetime, timedelta
import time
import urllib.request
import urllib.error
import socket
import random
import json

def add_years(d, years):
    """Return a date that's `years` years after the date (or before if negative).
    Handles leap year edge cases by falling back to Feb 28 if needed."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, day=28)

def get_winter_dates(december_year):
    """
    Returns start_date and end_date for winter season 
    (December of given year through February of next year)
    :param december_year: The year of the December start (e.g., 2020 for winter 2020-2021)
    """
    start_date = datetime(december_year, 12, 1).date()
    # March 1 minus 1 day gives us last day of February (handles leap years automatically)
    end_date = datetime(december_year + 1, 3, 1).date() - timedelta(days=1)
    # if end date is in the future, cap it to yesterday
    today = datetime.utcnow().date()
    if end_date >= today:
        end_date = today - timedelta(days=1)
    return start_date, end_date

def fetch_with_retry(url, headers):
    """
    Single-attempt fetch with delay for MISO historical data.
    Returns None immediately on quota exhaustion.
    """
    # Mandatory delay for server stability (conservative but safe)
    time.sleep(1)

    try:
        req = urllib.request.Request(url, headers=headers)
        response = urllib.request.urlopen(req, timeout=60)

        if response.getcode() == 200:
            raw_data = response.read().decode('utf-8')
            return json.loads(raw_data)
        else:
            print(f"  Warning: HTTP {response.getcode()}")
            return None

    except urllib.error.HTTPError as e:
        if e.code == 503:
            print(f"  503: Daily quota exceeded. Wait 6+ hours before resuming.")
            return None  # Fail fast, don't retry

        elif e.code == 429:
            print(f"  429: Rate limited. Waiting 60s then retrying once...")
            time.sleep(60)
            # Single retry after rate limit cooldown
            return fetch_with_retry(url, headers)  # Recursive call

        elif e.code == 404:
            return None

        else:
            print(f"  HTTP {e.code}: {e}")
            return None

    except socket.timeout:
        print(f"  Timeout error")
        return None

    except Exception as e:
        print(f"  Error: {str(e)}")
        return None