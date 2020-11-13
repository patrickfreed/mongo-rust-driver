use std::{collections::HashMap, sync::Arc};

use approx::abs_diff_eq;
use serde::Deserialize;

use crate::{
    options::StreamAddress,
    sdam::{description::topology::server_selection, Server},
    selection_criteria::ReadPreference,
    test::run_spec_test,
};

use super::TestTopologyDescription;

#[derive(Debug, Deserialize)]
struct TestFile {
    description: String,
    topology_description: TestTopologyDescription,
    mocked_topology_state: Vec<TestServer>,
    iterations: u32,
    outcome: TestOutcome,
}

#[derive(Debug, Deserialize)]
struct TestOutcome {
    tolerance: f64,
    expected_frequencies: HashMap<StreamAddress, f64>,
}

#[derive(Debug, Deserialize)]
struct TestServer {
    address: StreamAddress,
    operation_count: u32,
}

async fn run_test(test_file: TestFile) {
    println!("Running {}", test_file.description);

    let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();

    let servers: HashMap<StreamAddress, Arc<Server>> = test_file
        .mocked_topology_state
        .into_iter()
        .map(|desc| {
            (
                desc.address.clone(),
                Arc::new(Server::new_mocked(desc.address, desc.operation_count)),
            )
        })
        .collect();

    let topology_description = test_file
        .topology_description
        .into_topology_description(None)
        .unwrap();

    let read_pref = ReadPreference::Nearest {
        options: Default::default(),
    }
    .into();

    for _ in 0..test_file.iterations {
        let selection =
            server_selection::attempt_to_select_server(&read_pref, &topology_description, &servers)
                .expect("selection should not fail")
                .expect("a server should have been selected");
        *tallies.entry(selection.address.clone()).or_insert(0) += 1;
    }

    for (address, expected_frequency) in test_file.outcome.expected_frequencies {
        let actual_frequency =
            tallies.get(&address).cloned().unwrap_or(0) as f64 / (test_file.iterations as f64);

        let epsilon = if expected_frequency != 1.0 && expected_frequency != 0.0 {
            test_file.outcome.tolerance
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
