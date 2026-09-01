from pathlib import Path

from orderflow.local_spark.paths import DATA_ROOT, PROJECT_ROOT, data_path


def test_local_data_root_is_resolved_from_repository_root() -> None:
    assert PROJECT_ROOT.is_absolute()
    assert (PROJECT_ROOT / "pyproject.toml").is_file()
    assert DATA_ROOT == PROJECT_ROOT / "data"


def test_data_path_is_absolute_and_independent_of_working_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    assert data_path("silver", "marketing_campaigns") == (
        PROJECT_ROOT / "data" / "silver" / "marketing_campaigns"
    )
    assert data_path("silver", "marketing_campaigns").is_absolute()
