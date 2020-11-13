use std::{collections::HashMap, sync::Arc};

use approx::abs_diff_eq;
use serde::Deserialize;

use crate::{
    options::StreamAddress,
    sdam::{description::topology::server_selection, Server},
    test::run_spec_test,
};

#[derive(Debug, Deserialize)]
struct TestFile {
    description: String,
    in_window: Vec<TestPoolDescription>,
    expected_frequencies: HashMap<StreamAddress, f64>,
}

#[derive(Debug, Deserialize)]
struct TestPoolDescription {
    address: StreamAddress,
    operation_count: u32,
}

const TEST_ITERATIONS: u32 = 10000;

async fn run_test(test_file: TestFile) {
    let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();

    let servers: Vec<Arc<Server>> = test_file
        .in_window
        .into_iter()
        .map(|desc| Arc::new(Server::new_mocked(desc.address, desc.operation_count)))
        .collect();

    for _ in 0..TEST_ITERATIONS {
        let selection =
            server_selection::select_server_in_latency_window(servers.iter().collect(), false)
                .unwrap();
        *tallies.entry(selection.address.clone()).or_insert(0) += 1;
    }

    for (address, expected_frequency) in test_file.expected_frequencies {
        let actual_frequency =
            tallies.get(&address).cloned().unwrap_or(0) as f64 / (TEST_ITERATIONS as f64);

        let epsilon = if expected_frequency != 1.0 && expected_frequency != 0.0 {
            0.02
        } else {
            f64::EPSILON
        };

        assert!(
            abs_diff_eq!(actual_frequency, expected_frequency, epsilon = epsilon),
            "{}: for server {} expected frequency = {}, actual = {}",
            test_file.description,
            address,
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
