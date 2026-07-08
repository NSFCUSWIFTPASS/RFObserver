"""Enable `python -m rfobserver ...` as an alias for the `rfobserver` script.

Useful when the console-script entry point in `~/.local/bin` is not on PATH.
"""

from rfobserver.cli import main

if __name__ == "__main__":
    main()
