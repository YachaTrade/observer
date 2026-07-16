# Lessons

같은 실수를 반복하지 않기 위한 기록. 최신이 위로.

## 2026-06-16 — `cargo fmt`(인자 없음)는 크레이트 전체를 재포맷한다

**상황**: price_usd 스트림 변경 후 `cargo fmt`를 인자 없이 실행 → v2가 rustfmt-clean이 아니어서 **67개 파일 / +3343 -1738** 의 거대한 무관 diff 발생. 내 feature diff가 파묻힘 (Rule 3 surgical 위반).

**원인**: 이 레포는 rustfmt를 CI에서 강제하지 않아 committed 코드에 포맷 드리프트가 쌓여 있음. 전체 `cargo fmt`가 그걸 전부 canonical로 재작성.

**복구**: 무관 파일은 전부 rustfmt-only(의미 동일)임을 확인(`git diff -w` + "유일하게 돌린 게 fmt뿐") 후 `git diff --name-only v2 | grep -v <내 파일> | xargs git checkout v2 --`로 되돌리고, 내가 편집한 파일만 유지. 이후 재빌드+재테스트로 무결성 확인.

**규칙**:
- 변경한 파일만 포맷: `rustfmt <touched files>` 또는 `cargo fmt -- <files>`. **절대 인자 없는 `cargo fmt`로 전체를 밀지 말 것.**
- 커밋 전 `git diff --stat`로 파일 수를 반드시 확인 — 의도한 파일 수와 다르면 멈추고 원인 규명.
- 포맷 드리프트 일괄 정리가 필요하면 그건 **별도 PR**로 분리(기능 PR과 섞지 않는다).
