import os
import csv
import logging
from typing import List, Optional
from google_play_scraper import reviews, Sort

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_reviews_batch(
    app_id: str,
    lang: str = 'id',
    country: str = 'id',
    sort: Sort = Sort.MOST_RELEVANT,
    count: int = 30000,
    batch_size: int = 500
) -> List[dict]:
    """Fetch reviews in batches until target count is reached or no more data."""
    all_reviews = []
    token: Optional[str] = None

    while len(all_reviews) < count:
        current_count = min(batch_size, count - len(all_reviews))
        logging.info(f"Fetching {current_count} reviews... ({len(all_reviews)}/{count})")

        try:
            batch, token = reviews(
                app_id=app_id,
                lang=lang,
                country=country,
                sort=sort,
                count=current_count,
                continuation_token=token
            )
            if not batch:
                logging.info("No more reviews found.")
                break
            all_reviews.extend(batch)
            if token is None:
                break
        except Exception as e:
            logging.error(f"Error while fetching reviews: {e}")
            break

    logging.info(f"Total reviews fetched: {len(all_reviews)}")
    return all_reviews


def write_csv(data: List[dict], filepath: str):
    """Write list of dictionaries to a CSV file."""
    if not data:
        logging.warning("No data to write to CSV.")
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    headers = sorted(set().union(*(d.keys() for d in data)))

    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        logging.info(f"Data successfully saved to {filepath}")
    except Exception as e:
        logging.error(f"Failed to write CSV: {e}")


def main():
    config = {
        "app_id": "com.traveloka.android",
        "lang": "id",
        "country": "id",
        "sort": Sort.MOST_RELEVANT,
        "count": 30000
    }
    output_path = 'data/review.csv'

    reviews_data = fetch_reviews_batch(**config)
    write_csv(reviews_data, output_path)


if __name__ == "__main__":
    main()
