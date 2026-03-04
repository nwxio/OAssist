import logging
import time

from app.config import get_settings
from app.sync import run_full_sync


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> None:
    settings = get_settings()
    logging.info("oassist-worker started, sync interval=%ss", settings.sync_interval_seconds)
    while True:
        try:
            result = run_full_sync(settings)
            logging.info(
                "sync complete: indexed_documents=%s indexed_chunks=%s",
                result["indexed_documents"],
                result["indexed_chunks"],
            )
        except Exception as exc:
            logging.exception("sync failed: %s", exc)
        time.sleep(settings.sync_interval_seconds)


if __name__ == "__main__":
    main()
