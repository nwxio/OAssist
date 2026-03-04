from app.config import get_settings
from app.sync import run_full_sync


def main() -> None:
    settings = get_settings()
    result = run_full_sync(settings)
    print(result)


if __name__ == "__main__":
    main()
