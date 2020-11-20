use std::time::Duration;

use semver::VersionReq;
use tokio::sync::RwLockWriteGuard;

use crate::test::{
    CmapEvent,
    Event,
    EventClient,
    FailCommandOptions,
    FailPoint,
    FailPointMode,
    TestClient,
    CLIENT_OPTIONS,
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_pool_management() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);
    // let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    // if !setup_client.supports_fail_command().await {
    //     println!("skipping sdam_pool_management test due to server not supporting fail points");
    //     return;
    // }

    let mut options = setup_client_options;
    options.app_name = Some("SDAMPoolManagementTest".to_string());
    options.heartbeat_freq = Some(Duration::from_millis(50));
    let client =
        EventClient::with_additional_options(Some(options), Some(Duration::from_millis(50)), None)
            .await;
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

    println!("fail point enabled");

    subscriber
        .wait_for_event(Duration::from_secs(2000), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    subscriber
        .wait_for_event(Duration::from_secs(10), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");
}
