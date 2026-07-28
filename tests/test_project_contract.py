from pathlib import Path


def test_course_is_not_embedded() -> None:
    assert not Path("course").exists()


def test_source_package_exists() -> None:
    assert Path("src/minikafka").is_dir()
