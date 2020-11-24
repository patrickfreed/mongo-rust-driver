use std::time::{Duration, Instant};

use bson::doc;
use tokio::sync::RwLockWriteGuard;

use crate::{
    test::{
        CmapEvent,
        Event,
        EventClient,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    Client,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_pool_management() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.app_name = Some("SDAMPoolManagementTest".to_string());
    options.heartbeat_freq = Some(Duration::from_millis(50));

    let client =
        EventClient::with_additional_options(Some(options), Some(Duration::from_millis(50)), None)
            .await;

    if !client.supports_fail_command().await {
        println!("skipping sdam_pool_management test due to server not supporting fail points");
        return;
    }

    let mut subscriber = client.subscribe_to_events();

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMPoolManagementTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Times(1), fp_options);

    let _fp_guard = client
        .enable_failpoint(failpoint)
        .await
        .expect("enabling failpoint should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn min_heartbeat_frequency() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !setup_client.supports_fail_command().await {
        println!("skipping sdam_pool_management test due to server not supporting fail points");
        return;
    }

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMSleepTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Times(5), fp_options);

    let _fp_guard = setup_client
        .enable_failpoint(failpoint)
        .await
        .expect("enabling failpoint should succeed");

    let mut options = setup_client_options;
    options.app_name = Some("SDAMSleepTest".to_string());
    options.server_selection_timeout = Some(Duration::from_secs(10));
    let client = Client::with_options(options).expect("client creation succeeds");

    let start = Instant::now();
    client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .expect("ping should eventually succeed");

    let elapsed = Instant::now().duration_since(start).as_millis();
    assert!(
        elapsed >= 2000,
        "expected to take at least 2 seconds, instead took {}ms",
        elapsed
    );
    assert!(
        elapsed <= 3500,
        "expected to take at most 3.5 seconds, instead took {}ms",
        elapsed
    );
}
