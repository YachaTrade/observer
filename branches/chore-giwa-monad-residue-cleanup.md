# chore/giwa-monad-residue-cleanup

## Purpose

GIWA Sepolia 신규 배포를 막거나 오염시키는 Monad 잔재를 정리한다: DefiLlama 체인 슬러그 env화(기본 `ethereum`), migration의 Monad quote 주소 DEFAULT/시드 → GIWA WETH predeploy 교체, env 계약 `WMON`→`WETH` rename, CI/check.sh GIWA 전환, `.env.example` 신설.

- 스펙: `docs/superpowers/specs/2026-07-17-giwa-monad-residue-cleanup-design.md`
- 플랜: `docs/superpowers/plans/2026-07-17-giwa-monad-residue-cleanup.md`

## Changes

- `93cfac2` feat: DefiLlama coin ref 슬러그를 `DEFILLAMA_CHAIN_SLUG` env(기본 `ethereum`)로 전환 + 테스트 픽스처 갱신
- `fd93966` chore: migration 9개 파일의 Monad quote 주소 DEFAULT/시드 → GIWA WETH predeploy(`0x4200...0006`) 교체, LVMON 시드 제거, 백필 UPDATE 보존
- `fbae1e1` chore: env 계약 `WMON` → `WETH` rename (내부 `WNATIVE_ADDRESS` static 불변)
- `b0a70c3` chore: CI를 GIWA Sepolia RPC로 전환, 죽은 env 4개 제거; check.sh RPC 파라미터화
- `3405bcb` docs: `.env.example` 신설(+`.gitignore` 예외), README/event-indexing 배포 변수 갱신, price stream 주석 체인 중립화

검증: `cargo build`, `--lib`(23), `price_usd_logic`(8), `price_usd_price_source`(6), `token_chain`(1), `giwa_runtime_contract`(5), `price_usd`(4) 전부 통과. fmt 신규 위반 0 (베이스라인 기존 위반 273개는 범위 외). 실행: Codex(gpt-5.6-sol medium) / 검증·커밋: orchestrator.

## Outcome

- (PR 시 작성)
