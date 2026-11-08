import sys

MESSAGE = (
    "Non implémenté : voir ADR-0003/0004/0005 et ROADMAP phase 6 "
    "(https://github.com/Geobatpo07/datahut-duckhouse/blob/main/docs/ROADMAP.md)."
)


def main() -> None:
    print(MESSAGE, file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
