from pathlib import Path

from backend.app.utils.home_content import DEFAULT_HOME_CONTENT, build_home_content


ROOT = Path(__file__).resolve().parents[1]


def test_build_home_content_pads_truncated_about_cards_and_seo_paragraphs():
    """Regression: ``home.html`` indexes fixed slots; partial CMS JSON must not 500."""

    merged = build_home_content(
        {
            "home_content": '{"about":{"cards":[{"title":"A","items":["x"]}]},"seo":{"paragraphs":["only one"]}}',
        }
    )
    assert len(merged["about"]["cards"]) >= len(DEFAULT_HOME_CONTENT["about"]["cards"])
    assert isinstance(merged["about"]["cards"][0]["items"], list)
    assert len(merged["seo"]["paragraphs"]) >= len(DEFAULT_HOME_CONTENT["seo"]["paragraphs"])
    assert merged["seo"]["paragraphs"][0] == "only one"


def test_home_template_defers_hero_video_loading():
    template = (ROOT / "app/templates/home.html").read_text(encoding="utf-8")
    assert 'rel="preload" as="video"' not in template
    assert 'preload="none"' in template
    assert 'data-src="{{ hero_videos[0] }}"' in template
    assert 'requestIdleCallback(loadVideo' in template
