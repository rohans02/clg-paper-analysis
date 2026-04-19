from __future__ import annotations

from publication_manager.enums import InputMethod
from publication_manager.system_checks import run_system_checks
from publication_manager.workflow import approve_submission, create_submission


def test_system_checks_runs(session):
    submission = create_submission(
        session=session,
        submitted_by="faculty1",
        source_input="manual",
        source_input_method=InputMethod.MANUAL,
        payload={
            "faculty_name": "Dr. X",
            "title": "Paper X",
            "category": "Scopus",
            "publication_type": "Journal",
            "pub_date": "2025-01-01",
        },
        confidence_score=0.8,
    )
    approve_submission(session, submission.id, "admin1")
    checks_df, summary = run_system_checks(session)
    assert not checks_df.empty
    assert summary["total_checks"] >= 1
    assert "db_connectivity" in checks_df["name"].tolist()

