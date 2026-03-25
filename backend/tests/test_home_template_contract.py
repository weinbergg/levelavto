from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_home_template_defers_hero_video_loading():
    template = (ROOT / "app/templates/home.html").read_text(encoding="utf-8")
    assert 'rel="preload" as="video"' not in template
    assert 'preload="none"' in template
    assert 'data-src="{{ hero_videos[0] }}"' in template
    assert 'requestIdleCallback(loadVideo' in template
