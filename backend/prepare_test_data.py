import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(SRC_ROOT))

from pipeline import Pipeline  # noqa: E402


def main() -> None:
    Pipeline().prepare_data()


if __name__ == "__main__":
    main()
