use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::seq::SliceRandom;
use rayon::prelude::*;

#[derive(Clone, Debug)]
struct MockEvent {
    block_number: u64,
    transaction_index: u64,
    log_index: u64,
}

fn generate_events(n: usize) -> Vec<MockEvent> {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    // 먼저 정렬된 상태로 생성
    let mut events: Vec<MockEvent> = (0..n)
        .map(|i| {
            let block = 1000000 + (i as u64 / 100);
            let tx_index = (i as u64 % 100) / 10;
            let log_index = i as u64 % 10;
            MockEvent {
                block_number: block,
                transaction_index: tx_index,
                log_index: log_index,
            }
        })
        .collect();

    // 완전히 섞기
    events.shuffle(&mut rng);
    events
}

fn sort_benchmark(c: &mut Criterion) {
    let sizes = vec![1_000, 10_000, 100_000, 1_000_000];

    let mut group = c.benchmark_group("event_sorting");

    for size in sizes {
        let events = generate_events(size);

        group.bench_with_input(BenchmarkId::new("sort_by", size), &size, |b, _| {
            b.iter(|| {
                let mut events_clone = events.clone();
                events_clone.sort_by(|a, b| {
                    (a.block_number, a.transaction_index, a.log_index).cmp(&(
                        b.block_number,
                        b.transaction_index,
                        b.log_index,
                    ))
                });
                black_box(events_clone)
            })
        });

        group.bench_with_input(BenchmarkId::new("par_sort_by", size), &size, |b, _| {
            b.iter(|| {
                let mut events_clone = events.clone();
                events_clone.par_sort_by(|a, b| {
                    (a.block_number, a.transaction_index, a.log_index).cmp(&(
                        b.block_number,
                        b.transaction_index,
                        b.log_index,
                    ))
                });
                black_box(events_clone)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, sort_benchmark);
criterion_main!(benches);
