from __future__ import annotations

from contextlib import contextmanager
from threading import RLock
from typing import Iterator


# The app runs one publishing process. All delivery and post-edit entry points
# share this lock so their database snapshots and media files stay consistent.
delivery_lock = RLock()


class DeliveryBusyError(RuntimeError):
    pass


@contextmanager
def publishing_guard(*, blocking: bool = True) -> Iterator[None]:
    if not delivery_lock.acquire(blocking=blocking):
        raise DeliveryBusyError(
            "Another post update or publishing task is in progress. Try again when it finishes."
        )
    try:
        yield
    finally:
        delivery_lock.release()


@contextmanager
def edit_guard(*, blocking: bool = False) -> Iterator[None]:
    with publishing_guard(blocking=blocking):
        yield
