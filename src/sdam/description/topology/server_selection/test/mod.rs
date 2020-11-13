use std::{collections::HashMap, time::Duration};

use bson::doc;
use semver::VersionReq;
use tokio::sync::RwLockWriteGuard;

use crate::{
    options::StreamAddress,
    runtime::AsyncJoinHandle,
    test::{
        EventClient,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    RUNTIME,
};

mod in_window;
mod logic;

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn load_balancing_test() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);
    let setup_client = TestClient::with_options(Some(setup_client_options)).await;

    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&setup_client.server_version) {
        println!(
            "skipping load_balancing_test test due to server not supporting blockConnection option"
        );
        return;
    }

    if !setup_client.is_sharded() {
        println!("skipping load_balancing_test test due to topology not being sharded");
        return;
    }

    if CLIENT_OPTIONS.hosts.len() != 2 {
        println!("skipping load_balancing_test test due to topology not having 2 mongoses");
        return;
    }

    let options = FailCommandOptions::builder()
        .block_connection(Duration::from_millis(500))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn, options);

    let fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let mut client = EventClient::new().await;

    /// min_share is the lower bound for the % of times the the less selected server
    /// was selected. max_share is the upper bound.
    async fn do_test(client: &mut EventClient, min_share: f64, max_share: f64) {
        client.command_events.write().unwrap().clear();

        let mut handles: Vec<AsyncJoinHandle<()>> = Vec::new();
        for _ in 0..10 {
            let collection = client
                .database("load_balancing_test")
                .collection("load_balancing_test");
            handles.push(
                RUNTIME
                    .spawn(async move {
                        for _ in 0..10 {
                            let _ = collection.find_one(None, None).await;
                        }
                    })
                    .unwrap(),
            )
        }

        futures::future::join_all(handles).await;

        let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();
        for event in client.get_command_started_events("find") {
            *tallies.entry(event.connection.address.clone()).or_insert(0) += 1;
        }

        assert_eq!(tallies.len(), 2);
        let mut counts: Vec<_> = tallies.values().collect();
        counts.sort();

        // verify that the lesser picked server (slower one) was picked less than 25% of the time.
        let share_of_selections = (*counts[0] as f64) / ((*counts[0] + *counts[1]) as f64);
        println!("selections: {}", share_of_selections);
        assert!(share_of_selections <= max_share);
        assert!(share_of_selections >= min_share);
    }

    do_test(&mut client, 0.05, 0.25).await;

    // disable failpoint and rerun, should be close to even split
    drop(fp_guard);
    do_test(&mut client, 0.40, 0.50).await;
}
