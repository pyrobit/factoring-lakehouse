import json

from factoring_lakehouse.datagen import Generator, write


def test_generator_is_deterministic():
    a = Generator(seed=7, n_invoices=120).build()
    b = Generator(seed=7, n_invoices=120).build()
    assert a["day1"].invoices == b["day1"].invoices
    assert a["day2"].debtors == b["day2"].debtors


def test_manifest_matches_written_files(tmp_path):
    manifest = write(tmp_path, seed=3, n_clients=6, n_debtors=20, n_invoices=150)
    for batch, meta in manifest.items():
        for entity, n in meta["rows"].items():
            lines = (tmp_path / batch / entity / "part-00000.jsonl").read_text().splitlines()
            assert len(lines) == n, (batch, entity)
    assert json.loads((tmp_path / "manifest.json").read_text()) == json.loads(json.dumps(manifest))


def test_planted_defects_are_disjoint():
    gen = Generator(seed=11, n_invoices=200)
    gen.build()
    d1 = gen.manifest["day1"]
    assert len(d1["rejected"]["invoices"]) == 9
    assert not set(d1["rejected"]["invoices"]) & set(d1["duplicates"]["invoices"])
    d2 = gen.manifest["day2"]
    assert not set(d2["stale_updates"]) & set(d2["settled_invoices"])
    assert len(d2["rating_changes"]) == 5
