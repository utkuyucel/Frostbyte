import time

import click


def test_click_progress():
    """Test click progress bar functionality."""
    with click.progressbar(
        range(100), label="Testing", fill_char="█", empty_char="░", show_pos=True, show_percent=True
    ) as bar:
        for i in bar:
            # Simulate some work
            time.sleep(0.01)


if __name__ == "__main__":
    test_click_progress()
