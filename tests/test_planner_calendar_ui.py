from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment


TEMPLATE_PATH = Path(__file__).parents[1] / "app/templates/scheduled_posts.html"


def render_planner_script(*, view: str = "workspace", posts: list | None = None) -> str:
    """Render the actual planner script without loading application configuration."""
    template = TEMPLATE_PATH.read_text(encoding="utf-8").split("<script>", 1)[1].split("</script>", 1)[0]
    environment = Environment()
    environment.globals["timezone_name"] = lambda: "America/Los_Angeles"
    environment.filters["datetime_local_input"] = lambda value: value.strftime("%Y-%m-%dT%H:%M")
    return environment.from_string(template).render(
        planner_view=view,
        posts=posts or [],
        persona_name_by_id={"persona-1": "Lynx"},
    )


def _post(scheduled_for: str | None, *, status: str = "scheduled") -> SimpleNamespace:
    return SimpleNamespace(
        id="post-1",
        persona_id="persona-1",
        body="Existing post",
        status=status,
        display_status=status,
        scheduled_for=datetime.fromisoformat(scheduled_for) if scheduled_for else None,
        attachments=[],
        target_account_ids=[],
        post_type="standard",
        giveaway=None,
        last_error=None,
        delivery_breakdown=SimpleNamespace(succeeded=[], failed=[], cancelled=[]),
    )


NODE_BROWSER_STUBS = r'''
const assert = require("node:assert/strict");
const NativeDate = Date;
const frozenNow = new NativeDate(2026, 9, 8, 12, 34, 0);
global.Date = class extends NativeDate {
  constructor(...args) { super(...(args.length ? args : [frozenNow.getTime()])); }
  static now() { return frozenNow.getTime(); }
};
class Element {
  constructor(id) {
    this.id = id;
    this.dataset = {};
    this.textContent = "";
    this.innerHTML = "";
    this.listeners = new Map();
    const names = new Set();
    this.classList = {
      add(...values) { values.forEach(value => names.add(value)); },
      remove(...values) { values.forEach(value => names.delete(value)); },
      contains(value) { return names.has(value); },
      toggle(value, force) {
        const enabled = force === undefined ? !names.has(value) : force;
        enabled ? names.add(value) : names.delete(value);
        return enabled;
      },
    };
  }
  addEventListener(name, callback) { this.listeners.set(name, callback); }
  click() { this.listeners.get("click")?.({preventDefault() {}}); }
  setAttribute() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
}
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, new Element(id));
  return elements.get(id);
}
const viewButtons = [element("calendar-view-week"), element("calendar-view-month")];
viewButtons[0].dataset.calendarView = "week";
viewButtons[1].dataset.calendarView = "month";
global.document = {
  body: {appendChild() {}},
  getElementById: element,
  querySelector(selector) { return selector === ".planner-workspace-page" ? element("planner-page") : null; },
  querySelectorAll(selector) { return selector === "[data-calendar-view]" ? viewButtons : []; },
};
global.window = {matchMedia() { return {matches: false}; }};
global.bootstrap = {Modal: class {show() {} hide() {}}};
function calendarDates() {
  return [...element("scheduled-calendar-grid").innerHTML.matchAll(/data-drop-date="([^"]+)"/g)]
    .map(match => match[1]);
}
'''


def _run_planner(*, view: str, posts: list | None = None, assertions: str) -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise planner calendar JavaScript.")
    source = NODE_BROWSER_STUBS + render_planner_script(view=view, posts=posts) + assertions
    result = subprocess.run(
        [node, "-"],
        input=source,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        env={**os.environ, "TZ": "America/Los_Angeles"},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("view", ["workspace", "month"])
@pytest.mark.parametrize(
    "posts",
    [
        [_post("2026-05-14T13:00", status="posted")],
        [_post("2027-02-11T09:00")],
        [],
        [_post(None, status="draft")],
    ],
    ids=["historical-post", "future-post", "empty-planner", "unscheduled-draft"],
)
def test_planner_opens_current_week_or_month_regardless_of_existing_posts(view, posts):
    _run_planner(view=view, posts=posts, assertions=r'''
assert.equal(toDateKey(plannerState.anchorDate), "2026-10-08");
assert.equal(element("scheduled-calendar-root").classList.contains("d-none"), false);
assert.equal(element("scheduled-calendar-empty").classList.contains("d-none"), true);
assert.match(element("scheduled-calendar-grid").innerHTML, /is-today/);
assert.notEqual(element("calendar-range-label").textContent, "No post plans yet");
if (plannerInitialView === "month") {
  assert.equal(plannerState.viewMode, "month");
  assert.equal(calendarDates().length, 42);
  assert.equal(calendarDates()[0], "2026-09-27");
  assert.equal(calendarDates().at(-1), "2026-11-07");
  assert.equal(element("calendar-range-label").textContent,
    new Intl.DateTimeFormat(undefined, {month: "long", year: "numeric"}).format(frozenNow));
} else {
  assert.equal(plannerState.viewMode, "week");
  assert.deepEqual(calendarDates(), [
    "2026-10-04", "2026-10-05", "2026-10-06", "2026-10-07",
    "2026-10-08", "2026-10-09", "2026-10-10",
  ]);
}
''')


@pytest.mark.parametrize("view", ["workspace", "month"])
def test_planner_today_button_returns_to_current_period_after_navigation(view):
    _run_planner(view=view, posts=[_post("2026-05-14T13:00")], assertions=r'''
const initialDates = calendarDates();
element("calendar-prev-button").click();
assert.notDeepEqual(calendarDates(), initialDates);
element("calendar-next-button").click();
assert.deepEqual(calendarDates(), initialDates);
element("calendar-next-button").click();
assert.notDeepEqual(calendarDates(), initialDates);
element("calendar-today-button").click();
assert.equal(toDateKey(plannerState.anchorDate), "2026-10-08");
assert.deepEqual(calendarDates(), initialDates);
assert.match(element("scheduled-calendar-grid").innerHTML, /is-today/);
''')


@pytest.mark.parametrize(
    ("date", "direction", "expected"),
    [
        ("2026-01-31", 1, "2026-02-01"),
        ("2024-01-31", 1, "2024-02-01"),
        ("2024-02-29", 1, "2024-03-01"),
        ("2024-03-31", -1, "2024-02-01"),
        ("2026-12-31", 1, "2027-01-01"),
        ("2026-01-31", -1, "2025-12-01"),
    ],
)
def test_month_navigation_does_not_skip_short_months_or_year_boundaries(date, direction, expected):
    _run_planner(view="month", assertions=f'''
plannerState.anchorDate = parseDateKey("{date}");
renderCalendar();
element("calendar-{'next' if direction > 0 else 'prev'}-button").click();
assert.equal(toDateKey(plannerState.anchorDate), "{expected}");
assert.equal(element("calendar-range-label").textContent,
  new Intl.DateTimeFormat(undefined, {{month: "long", year: "numeric"}}).format(parseDateKey("{expected}")));
''')
