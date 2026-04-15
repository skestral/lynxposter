from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import Base


@pytest.fixture(autouse=True)
def block_outbound_http(monkeypatch):
    def guarded_request(self, method, url, *args, **kwargs):
        raise AssertionError(f"Unexpected outbound HTTP request in tests: {method} {url}")

    monkeypatch.setattr(requests.sessions.Session, "request", guarded_request)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    TestingSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)
    session = TestingSession()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()
