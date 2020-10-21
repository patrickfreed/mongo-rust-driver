use approx::{abs_diff_eq, abs_diff_ne};
use bson::Document;
use serde::Deserialize;

use crate::test::run_spec_test;
use rand::prelude::{IteratorRandom, SliceRandom};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TestFile {
    description: String,
    in_window: Vec<TestPoolDescription>,
    expected_frequencies: HashMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct TestPoolDescription {
    id: String,
    active_connection_count: u32,
    available_connection_count: u32,
}

fn select_server(in_window: &Vec<TestPoolDescription>) -> String {
    if in_window.len() == 1 {
        return in_window[0].id.clone();
    }

    let mut rng = rand::thread_rng();
    let mut choices = in_window.iter().choose_multiple(&mut rng, 2);
    choices.shuffle(&mut rng);

    let server1 = choices[0];
    let server2 = choices[1];

    if abs_diff_ne!(
        server1.active_connection_count,
        server2.active_connection_count,
        epsilon = 1
    ) {
        choices
            .iter()
            .min_by_key(|server| server.active_connection_count)
            .unwrap()
            .id
            .clone()
    } else {
        choices
            .iter()
            .max_by_key(|server| server.available_connection_count)
            .unwrap_or(&server1)
            .id
            .clone()
    }
}

async fn run_test(test_file: TestFile) {
    let mut tallies: HashMap<String, u32> = HashMap::new();

    for _ in 0..1000 {
        let selection = select_server(&test_file.in_window);
        *tallies.entry(selection).or_insert(0) += 1;
    }

    for (id, expected_frequency) in test_file.expected_frequencies {
        let actual_frequency = tallies.get(&id).cloned().unwrap_or(0) as f64 / 1000.0;

        let epsilon = if expected_frequency != 1.0 && expected_frequency != 0.0 {
            0.05
        } else {
            f64::EPSILON
        };

        assert!(
            abs_diff_eq!(actual_frequency, expected_frequency, epsilon = epsilon),
            "{}: for server {} expected frequency = {}, actual = {}",
            test_file.description,
            id,
            expected_frequency,
            actual_frequency
        );
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn select_in_window() {
    run_spec_test(&["server-selection", "in_window"], run_test).await;
}
